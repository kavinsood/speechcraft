# Reference Picker Spec

## Purpose

This document defines the complete feature spec for the Reference Picker branch as it should fit into the current Speechcraft repository, not an imaginary greenfield rewrite.

The feature goal is:

- mine high-value reference spans from project audio
- let the operator discover moods / styles quickly
- let the operator steer results with positive and negative feedback
- support subspan refinement before saving
- save approved references as reusable managed assets
- preserve provenance and non-destructive behavior

This document also intentionally surfaces the implementation tensions that exist in the repo today.

## Current Repo Reality

Speechcraft today is still a Phase 1 slice-review workstation.

What exists now:

- top-level app shell in `frontend/src/App.tsx`
- `ingest`, `label`, and a first-pass `reference` page now exist
- `label` is built around `Slice`, `AudioVariant`, and `EditCommit`
- source recordings now exist on both backend and frontend
- Clip Lab behavior exists only as slice-scoped variant creation / switching
- `ReferenceAsset` and `ReferenceVariant` now exist as real library objects
- there is still no generic background job layer yet
- export and artifact warmup are still synchronous

That means the Reference Picker is not a tiny add-on.

It is the first feature in this repo that:

- works directly from `SourceRecording` on the frontend
- needs its own discovery pipeline and run history
- wants a shared processing surface without being a `Slice`
- forces `ReferenceAsset` to become a real object

## Product Stance

The guiding stance for this branch is:

- discovery is not selection
- clustering is a discovery aid, not the core ranking mechanism
- the real product value is intent-aware reranking over candidate spans
- the picker must operate on spans, not only pre-existing slice boundaries
- references are unusually high leverage, so subspan refinement is mandatory
- approved references must become first-class managed assets
- the label queue must stay slice-centric and must not be polluted with reference-only pseudo-slices

## Current Verdict

The architecture direction is fundamentally sound, but the branch must ship a narrow Phase 1.

The spine of the feature is:

- span-first candidate selection
- intent-aware reranking
- clustering as discovery, not truth
- trim and promote into real saved reference assets

The document should not be read as permission to build every future surface immediately. Anything beyond that spine must justify itself against branch scope, current repo complexity, and implementation risk.

## Current Implementation Status

As of `2026-03-20`, the branch has crossed the foundation threshold but has not yet started the real candidate-run layer.

### Completed

- dedicated `reference` route exists in the app shell
- source-recording list endpoint and source-selection UI exist
- `ReferenceAsset` + `ReferenceVariant` exist as first-class managed objects
- save-current-slice-state from the label workstation into the shared reference library exists
- reference library list / detail behavior exists
- reference variant media serving exists
- duplicate-intent handling exists in the label inspector:
  - fast save for a new slice state
  - `Open Existing` for an already-saved state
  - `Save Another...` with optional `name` / `mood_label`
- legacy thin-reference migration exists
- unresolved legacy migration rows produce a durable report and can be retried on later startup
- reference-variant storage uses media-root-relative keys rather than absolute paths
- repository-side integrity validation exists for reference provenance and active-variant membership
- repository instantiation is lazy instead of import-time eager

### Partially Complete

- `ReferencePickerRun` schema exists, but the real run APIs, worker loop, and polling contract do not
- the `reference` route is a real library shell, but not yet a true candidate picker workstation

### Not Started

- run creation / listing / status polling
- candidate generation over source recordings
- candidate manifests, embeddings, and preview cache
- ranked candidate list
- positive / negative rerank loop
- browser-side candidate auto-trim
- candidate promotion from picker-run output
- cluster / mood discovery lens
- reference-specific processing surface

### Completedness Estimate

Rough estimate against this document:

- about `45%` of the real Phase 1 ship target is done
- about `25%` to `30%` of the full multi-phase spec is done

These are deliberately conservative. The hard foundation is in place, but most of the picker-specific behavior is still ahead.

### Verification Completed So Far

- backend: `./.venv/bin/python -m unittest -v tests.test_repository_media`
- frontend: `npm run build`

The full repo test matrix has not been run yet.

## Goals

The feature is only successful if it does these well:

- select one or more project source recordings as candidate mining input
- generate candidate spans that are useful for reference selection, not just dataset slicing
- let the operator shape ranking with positive and negative feedback
- let the operator preview, trim, and promote a candidate quickly
- save approved references as durable managed objects
- allow label-workstation variants to enter the same reference library

## Non-Goals

This branch should not attempt to solve all of the following:

- full upstream preprocessing orchestration
- a generic cross-domain asset abstraction for every workstation object
- inference probe generation tied to live model execution
- a universal background job framework for the whole product
- full alignment-aware reference segmentation
- replacing the Phase 1 labeling model

These can be designed for, but should not block the first usable implementation.

## Delivery Phases

The spec must distinguish what ships first from what belongs later.

### Phase 1: First Shippable Version

Phase 1 includes:

- dedicated `reference` route
- source recording list endpoint and source-selection UI
- `ReferencePickerRun` row and run status polling
- candidate generation over one or more source recordings
- ranked candidate list
- positive / negative rerank
- candidate preview
- browser-side auto-trim suggestion
- candidate promotion to `ReferenceAsset`
- saved reference library list
- save-from-label into the same reference library

Phase 1 explicitly does not require:

- cluster browse as a primary surface
- full Reference Lab workstation parity
- persistent rerank sessions
- rich mood taxonomy
- elaborate metric families
- archive / duplicate / restore flows
- generalized platform-wide job infrastructure

### Phase 2: Discovery And Library Depth

Phase 2 includes:

- cluster / mood discovery lens
- richer library browsing and filtering
- better asset detail views
- reference-specific processing surface
- variant compare / switch improvements

### Phase 3: Deeper Modeling

Phase 3 may include:

- richer style embeddings
- saved rerank sessions
- cross-run reuse and caching improvements
- smarter provenance graph tooling
- probe synthesis / validation workflows

## Core Definitions

### Source Recording

The immutable long-form audio file that candidate spans come from.

### Reference Picker Run

A discovery run over one project scope, usually one or more `SourceRecording` rows. A run owns:

- config
- status
- artifact paths
- candidate manifest
- embeddings
- optional cluster summaries

It does not own the final approved references.

### Candidate Span

A derived proposal inside a run:

- source-relative start / end
- transcript snippet if available
- quality metrics
- speaker purity metrics
- homogeneity metrics
- cluster id
- risk flags
- score views

Candidate spans are derived artifacts, not durable library assets.

### Reference Asset

A durable saved reference object in the project library. A reference asset must:

- own its managed audio media
- preserve provenance back to source audio
- support multiple saved variants over time
- be usable regardless of whether it came from the label flow or picker flow

### Reference Variant

An immutable physical WAV attached to a `ReferenceAsset`.

Variants allow:

- original promoted trim
- processed outputs
- later retrims or alternate renders

### Reference Lab

A reference-scoped processing and comparison surface. It should visually and behaviorally resemble Clip Lab, but in the repo today it should be treated as a reference-asset workstation, not as a `Slice`.

## Primary Product Flows

### Flow 1: Mine From Raw Recordings

1. User opens the Reference Picker page for a project.
2. User selects one or more source recordings.
3. User chooses discovery mode and candidate parameters.
4. User starts a picker run.
5. Backend creates the run, computes artifacts, and marks it complete.
6. User browses ranked candidates and, later, optional discovered clusters.
7. User previews candidates, likes / dislikes some of them, and reranks.
8. User opens a candidate detail view.
9. UI auto-suggests tighter trim bounds.
10. User accepts or adjusts trim.
11. User promotes the span to a `ReferenceAsset`.
12. User optionally opens the saved reference in Reference Lab for processing.

### Flow 2: Save Current Slice State From Label Workstation

1. User reviews a slice in the label workstation.
2. User decides the current rendered slice state is useful as a reference.
3. User clicks `Save Current Slice State`.
4. Backend creates a new `ReferenceAsset` and initial `ReferenceVariant` from the rendered slice audio and current edit state.
5. The new asset appears in the project reference library.

### Flow 3: Process A Saved Reference

1. User opens a saved reference asset from the library.
2. User previews the active reference variant.
3. User runs a processing model such as `deepfilternet`.
4. Backend creates a new `ReferenceVariant`.
5. User compares and activates the preferred variant.

## Information Architecture

## Recommendation

Add a new top-level route and step:

- `reference`

The step order becomes:

- `ingest`
- `enhance`
- `segment`
- `label`
- `reference`
- `train`
- `deploy`

### Why This Is Recommended

- The feature is too large to hide inside `label`.
- It is not semantically the same as `segment`.
- It will be the first serious consumer of `SourceRecording`.
- It needs its own history, filters, and library concepts.
- It avoids turning the label workstation into a second workstation with different objectives.

### Tension

The current app shell is a linear stepper, while reference selection is not purely linear. It is more like a sidecar workstation that can be used before train and before inference. Even so, a dedicated top-level route is still the least confusing implementation.

Important clarification:

- the dedicated route is product truth
- the stepper placement is a UI compromise in the current app shell

Do not mistake the stepper ordering for the conceptual workflow ordering of the product.

## Frontend Architecture

## Existing Files That Must Change

- `frontend/src/App.tsx`
- `frontend/src/api.ts`
- `frontend/src/types.ts`
- `frontend/src/pages/LabelPage.tsx`
- `frontend/src/workspace/InspectorPane.tsx`

## New Frontend Files Recommended

- `frontend/src/pages/ReferencePage.tsx`
- `frontend/src/reference/ReferenceRunSidebar.tsx`
- `frontend/src/reference/ReferenceCandidateList.tsx`
- `frontend/src/reference/ReferenceCandidateCard.tsx`
- `frontend/src/reference/ReferenceCandidateDetail.tsx`
- `frontend/src/reference/ReferenceFiltersPane.tsx`
- `frontend/src/reference/ReferenceLibraryPane.tsx`
- `frontend/src/reference/ReferenceLabPane.tsx`
- `frontend/src/reference/reference-helpers.ts`

### Reference Page Layout

Recommended three-column layout:

- left: run and source selection
- center: candidate results and detail
- right: filters, anchor controls, and saved library

The page should feel like the existing workstation style, not a popover stapled onto Label.

### Left Column

Responsibilities:

- choose project source recordings
- choose existing runs
- start a new run
- show run status and artifact stats

Data shown:

- available source recordings
- recording duration and derivation hint
- last completed runs
- run mode and config summary

### Center Column

Responsibilities:

- ranked results
- optional cluster browse later
- candidate preview
- trim refinement
- promote action

Views:

- ranked list view
- optional cluster / mood view later
- candidate detail drawer or panel

Candidate card fields:

- short transcript
- duration
- overall score
- quality safety indicators
- optional cluster / mood label
- risk flags
- like / dislike buttons
- open / preview button

### Right Column

Responsibilities:

- search and filters
- mode switch
- anchor state
- saved reference library

Controls:

- mode toggle: `zero_shot`, `finetune`, `both`
- optional cluster filter later
- minimum duration / maximum duration
- risk flag filters
- hide already promoted spans
- positive anchors summary
- negative anchors summary
- saved library list

## Reference Lab UI

The lab should visually reuse existing ideas from the slice editor:

- waveform pane
- transport
- variant history
- processing buttons
- metadata panel

But it should not pretend a reference asset is a slice.

In this repo, the practical move is:

- extract reusable presentational pieces where possible
- keep separate page-level state and separate API methods for references

Do not try to force `EditorPane.tsx` to serve both slices and references in one pass during the first implementation.

Phase 1 should keep this narrow:

- asset detail
- active variant preview
- variant list
- minimal processing hooks only if they are already cheap to support

A full reference-specific lab workstation belongs in Phase 2, not in the first shipping slice of the branch.

### Label Workstation Modifications

Add a reference save action in the label flow.

Recommended placement:

- active variant action in `InspectorPane.tsx`
- optional action in `EditorPane.tsx` toolbar later

The action should save the current rendered slice state into the reference library, not just mutate the slice.

## Backend Architecture

## Existing Files That Must Change

- `backend/app/models.py`
- `backend/app/main.py`
- `backend/app/repository.py`

## New Backend Test Files Recommended

- `backend/tests/test_reference_picker_runs.py`
- `backend/tests/test_reference_assets.py`
- `backend/tests/test_reference_picker_migrations.py`

## Core Backend Responsibilities

The backend must own:

- run creation and tracking
- candidate artifact loading
- reranking math
- preview media materialization
- reference asset creation
- reference variant processing
- cleanup semantics

## Run Storage Model

### Recommendation

Use a hybrid model:

- run metadata in SQLite
- candidate manifests and embeddings on disk under a run artifact root

### Why

Candidate runs are derived, numerous, and vector-heavy. SQLite is a poor home for large embedding blobs and transient candidate rows. The current repo already prefers managed files and manifests for heavyweight artifacts.

Practical caveat:

The UI still wants fast filtering and paging over small scalar fields. If manifest-only query behavior becomes clumsy, it is acceptable to add a lightweight candidate summary cache or indexed sidecar for scalar metadata later. Do not put embeddings into SQLite just to make filtering easy.

### Run Row Shape

Keep the run row boring.

Recommended fields:

- `id`
- `project_id`
- `status`
- `mode`
- `config`
- `artifact_root`
- `candidate_count`
- `error_message`
- `created_at`
- `started_at`
- `completed_at`

Do not turn the database into a path registry for every run artifact. Standard files should be derived conventionally from `artifact_root`.

### Run Artifact Layout

Recommended layout:

- `backend/data/reference-picker/runs/<run_id>/config.json`
- `backend/data/reference-picker/runs/<run_id>/manifest.json`
- `backend/data/reference-picker/runs/<run_id>/candidates.jsonl`
- `backend/data/reference-picker/runs/<run_id>/embeddings.npy`
- `backend/data/reference-picker/runs/<run_id>/cluster-summary.json`
- `backend/data/reference-picker/runs/<run_id>/preview-cache/<candidate_id>.wav`
- `backend/data/reference-picker/runs/<run_id>/preview-peaks/<candidate_id>-bins-960.json`

Run deletion may remove this directory safely after promoted references have copied their own media.

Persisted rerank session state is not required for Phase 1. Positive / negative anchors may remain client state, with the server accepting them in rerank requests and returning updated scores.

## Candidate Preview Strategy

Do not pre-render every candidate preview WAV eagerly.

Recommended behavior:

- previews are materialized lazily on first request
- cached under the run directory
- served with `FileResponse`
- reused for repeated audition

This mirrors the current slice render cache approach well enough.

## Candidate Generation Strategy

The feature should not depend on only one upstream artifact source.

### Input Scope

First implementation should support:

- one or more `SourceRecording` ids

Later it may support:

- slice variants
- saved references as similarity anchors

### Candidate Identity Stability

Candidate ids must be stable within a run and deterministic from canonical candidate properties.

Recommended ingredients:

- source media object id
- absolute start time
- absolute end time
- candidate generation scale or family label

Do not use manifest row index as candidate identity.

Candidate ids drive:

- rerank anchors
- preview URLs
- hide-promoted state
- promotion provenance
- cache filenames

### Candidate Scaffolding

The scaffolding stage is allowed to use VAD and similar heuristics, but only to propose candidate regions, not as the final truth of what the user saves.

Recommended initial pipeline:

1. normalize analysis copy to mono 16 kHz
2. run speech activity scaffold
3. merge short gaps
4. generate multi-scale overlapping candidate windows
5. compute metrics
6. compute embeddings
7. optionally cluster
8. write manifest

### Candidate Window Durations

Use multi-scale candidates instead of one fixed duration.

Recommended defaults:

- zero-shot focused windows: `3.0`, `4.5`, `6.0` seconds
- finetune steering windows: `2.5`, `4.0`, `5.0` seconds
- overlap stride: around 40 to 50 percent of the window

This is closer to the reference-selection goal than the old `12s-20s` heuristic windows.

### Candidate Dedup And Suppression

Multi-scale overlapping windows will create near-duplicates unless the run performs local suppression.

Phase 1 should include a simple non-maximum-suppression style pass using:

- temporal overlap threshold
- score ordering
- optional embedding-similarity threshold for nearly identical neighbors

The picker must not drown the operator in five versions of the same span shifted by 300 ms.

## Ranking Model

The ranking model must separate:

- discovery similarity
- acoustic quality
- speaker purity
- span homogeneity
- operator intent

Conceptually, the system must not collapse all of these into one magical embedding axis. Even if Phase 1 uses a limited number of actual models, scoring should still think in terms of distinct signals for:

- speaker identity
- style / delivery
- acoustic cleanliness / contamination

### Required Candidate Fields

Every manifest row should contain:

- `candidate_id`
- `run_id`
- `source_media_kind`
- `source_recording_id`
- `source_variant_id` if applicable
- `source_start_seconds`
- `source_end_seconds`
- `duration_seconds`
- `transcript_text`
- `speaker_name`
- `language`
- `embedding_index`
- `quality_metrics`
- `speaker_metrics`
- `homogeneity_metrics`
- `risk_flags`
- `default_scores`

Optional Phase 2 fields:

- `cluster_id`
- `cluster_score`
- richer style-metric detail

### Quality Metrics

Initial metrics should include:

- speech ratio
- clipping rate
- RMS / loudness sanity
- simple SNR proxy
- overlap suspicion if available
- silence-at-boundaries flags

### Speaker Purity Metrics

Initial metrics should include:

- intra-span speaker consistency
- anchor speaker similarity if anchor is provided
- multi-speaker suspicion if available

### Homogeneity Metrics

Homogeneity should be a broad concept, not just one prosody number.

Initial metrics may include:

- style embedding drift across subwindows if available
- energy contour instability
- speaking-rate instability if available
- obvious pitch / force discontinuity if available
- non-speech intrusion flags

Phase 1 does not need a research-grade metric zoo. It does need one useful rolled-up `homogeneity_score` or equivalent penalty source.

### Ranking Modes

The UI should expose three score views:

- `zero_shot`
- `finetune`
- `both`

Recommended weighting intent:

- `zero_shot`: safety, purity, homogeneity, then style
- `finetune`: style match, homogeneity, then quality
- `both`: balanced compromise

### Contrastive Rerank

Positive and negative anchors should be first-class.

Sources allowed as anchors:

- candidate ids from the current run
- saved `ReferenceAsset` ids
- active slice variants later

Initial rerank equation:

- `intent_score = cosine(candidate, positive_mean) - cosine(candidate, negative_mean)`

Final score combines:

- mode base score
- intent score
- quality bonus
- risk penalty

Clustering must not be required for reranking to work.

## Clustering

Clustering is optional discovery structure.

It should power:

- cluster browse
- discovered mood neighborhoods
- cluster-local quality normalization

It should not be treated as the authoritative answer to what the user wants.

### Recommendation

- cluster in normalized embedding space
- do not make the product depend on UMAP for ranking
- if a lower-dimensional view is needed, use it only for visualization

## Subspan Refinement

Subspan refinement is mandatory.

### Behavior

- when a user opens a candidate, the UI computes a tighter trim suggestion
- the suggestion strips leading and trailing silence and shallow non-speech tails
- the user can accept, drag, or reset

### Recommendation For First Implementation

Perform the trim suggestion in the browser using WebAudio.

Why:

- candidate previews are short
- this avoids adding a dedicated trim endpoint immediately
- it matches the UX critique directly
- it keeps the interaction fast

### Suggested Frontend Algorithm

1. decode candidate preview audio to `AudioBuffer`
2. compute short-time RMS over 10 to 20 ms windows
3. determine speech threshold relative to local statistics
4. apply small morphological closing / opening to remove spiky errors
5. choose the main voiced span
6. add tiny lead and tail margins
7. clamp to candidate bounds

Suggested margins:

- leading margin: 60 to 120 ms
- trailing margin: 120 to 200 ms

### Persistence Rule

Suggested trim bounds stay local until:

- promoted to a reference asset
- or explicitly saved into a reference asset update

Do not mutate the candidate manifest.

### Time Coordinate Contract

This contract must be explicit.

- candidate manifest bounds are canonical absolute bounds in the coordinate system of the source media object
- preview audio is derived from those canonical bounds
- browser trim suggestion is computed relative to the preview buffer
- before promotion, the browser must convert the final trim back into absolute source-relative bounds
- the backend validates and cuts using those absolute bounds

Do not persist preview-relative offsets as reference truth.

## Reference Asset Data Model

## Why The Existing Model Is Not Enough

Current `ReferenceAsset` cannot support the feature because it:

- only points to one `AudioVariant`
- enforces one asset per variant
- cannot store trimmed bounds
- cannot store its own managed media
- cannot support variants
- cannot support raw-source-picked references cleanly

## Recommended Schema

### ReferencePickerRun

New table.

Suggested fields:

- `id`
- `project_id`
- `status`
- `mode`
- `config`
- `artifact_root`
- `candidate_count`
- `error_message`
- `created_at`
- `started_at`
- `completed_at`

### ReferenceAsset

`ReferenceAsset` is the logical curated reference object.

Suggested fields:

- `id`
- `project_id`
- `name`
- `status` (`draft`, `active`, `archived`)
- `transcript_text`
- `speaker_name`
- `language`
- `mood_label`
- `notes`
- `favorite_rank`
- `active_variant_id`
- `created_from_run_id`
- `created_from_candidate_id`
- `model_metadata`
- `created_at`
- `updated_at`

`ReferenceAsset` may keep lightweight origin summary fields later, but it must not be the sole authoritative holder of exact source-span lineage once multiple variants exist.

### ReferenceVariant

New table.

Suggested fields:

- `id`
- `reference_asset_id`
- `source_kind` (`source_recording`, `slice_variant`, `reference_variant`)
- `source_recording_id`
- `source_slice_id`
- `source_audio_variant_id`
- `source_reference_variant_id`
- `source_start_seconds`
- `source_end_seconds`
- `file_path`
- `is_original`
- `generator_model`
- `sample_rate`
- `num_samples`
- `model_metadata`
- `deleted`
- `created_at`

### Why This Model

It preserves the correct split:

- logical reference asset
- concrete saved media variants

It also avoids forcing picker output through `Slice`.

Critical rule:

- `ReferenceAsset` owns user-facing intent and active-variant selection
- `ReferenceVariant` owns authoritative concrete-media lineage

If later retrims or processed outputs are allowed, exact source-span provenance must be read from the variant, not inferred from the asset.

### Provenance Enforcement

Variant provenance uses a one-of shape and must be validated accordingly.

Required invariants:

- exactly one source mode is active for a variant
- all referenced source entities belong to the same project
- `active_variant_id` must belong to the parent asset
- `source_start_seconds` / `source_end_seconds` must only be set when meaningful for that source mode

## Minimal Alternative

There is a smaller schema alternative:

- keep `ReferenceAsset`
- add `start_seconds` and `end_seconds`
- drop uniqueness on `audio_variant_id`

This is not recommended as the complete feature shape because it breaks down once raw-source promotion and processed reference variants appear.

## Storage Layout For Saved References

Recommended layout:

- `backend/data/media/reference-variants/<variant_id>.wav`

Optional if per-asset folders are preferred later:

- `backend/data/media/reference-assets/<asset_id>/<variant_id>.wav`

For consistency with current media serving, the flat `reference-variants` directory is simpler.

## API Contract

## Project And Source Discovery

Add:

- `GET /api/projects/{project_id}/source-recordings`

This should return enough metadata to choose picker input:

- id
- parent_recording_id
- processing_recipe
- sample_rate
- num_channels
- num_samples
- duration

## Run Endpoints

Add:

- `GET /api/projects/{project_id}/reference-runs`
- `POST /api/projects/{project_id}/reference-runs`
- `GET /api/reference-runs/{run_id}`
- `GET /api/reference-runs/{run_id}/candidates`
- `POST /api/reference-runs/{run_id}/rerank`
- `GET /media/reference-candidates/{run_id}/{candidate_id}.wav`

Recommended request model for run creation:

- selected recording ids
- mode
- target duration preferences
- candidate count cap
- whether optional clustering is enabled
- optional seed anchor ids

Recommended query params for candidate listing:

- `view=ranked`
- `query`
- `offset`
- `limit`
- `sort`
- `risk`
- `mode`

Optional Phase 2 query params:

- `view=cluster`
- `cluster_id`

Recommended rerank request:

- positive candidate ids
- negative candidate ids
- positive reference asset ids
- negative reference asset ids
- mode

## Reference Asset Endpoints

Add:

- `GET /api/projects/{project_id}/reference-assets`
- `GET /api/reference-assets/{asset_id}`
- `POST /api/reference-assets/from-candidate`
- `POST /api/reference-assets/from-slice`
- `PATCH /api/reference-assets/{asset_id}`
- `PATCH /api/reference-assets/{asset_id}/active-variant`
- `POST /api/reference-assets/{asset_id}/variants/run`
- `GET /media/reference-variants/{variant_id}.wav`

Optional later:

- `POST /api/reference-assets/{asset_id}/duplicate`
- `POST /api/reference-assets/{asset_id}/archive`
- `POST /api/reference-assets/{asset_id}/restore`

## Promotion Semantics

Promotion from candidate must:

- extract the chosen trim bounds from the exact media object the candidate was derived from
- write a managed WAV for the new reference variant
- create the logical `ReferenceAsset`
- create the initial `ReferenceVariant`
- set the asset's active variant
- preserve provenance back to the candidate run and source media lineage

Do not silently jump from a derivative-backed candidate preview to a raw-parent source cut unless the UI explicitly asked for that policy.

Promotion from current slice state must:

- render the current slice state into managed reference media
- preserve provenance to `Slice`, `AudioVariant`, and active edit state
- not create hidden slices

## Reference Lab Processing

Processing a reference asset variant should parallel current slice variant runs:

- copy source variant
- run transform
- create new `ReferenceVariant`
- set active variant only if the user explicitly chooses so

Do not auto-promote processed outputs silently.

## Media And Waveform Behavior

### Candidate Preview

Use browser decode for first implementation.

Why:

- candidate previews are short
- this avoids new backend peaks routes
- `WaveformPane.tsx` can already render from audio URL

### Saved Reference Assets

For Reference Lab, browser decode is still acceptable initially because assets are short. Backend peaks caching can be added later if needed.

### Important Distinction

Do not couple reference waveform support to slice waveform routes. The current `GET /api/clips/{clip_id}/waveform-peaks` contract is slice-specific and should stay that way.

## Backend Processing And Jobs

## Recommendation

Treat picker runs as asynchronous work even if the first implementation cheats for local development.

### Why

- long recordings make synchronous request handling a bad fit
- the repo already has an async-export tension
- picker runs are even more compute-heavy than export preview

### Practical Today-Level Decision

For this repo today, one of two paths must be chosen explicitly:

#### Option A: Synchronous MVP

- easiest to wire quickly
- acceptable only for small local testing
- known wrong shape for long-form sources

#### Option B: Run Row Plus Lightweight Worker

- more correct
- requires a simple polling model
- aligns better with future export / artifact job work

This spec recommends Option B for the real feature, even if an internal first pass starts with Option A.

### Required Run State Machine

If Option B is used, the run contract must be explicit.

Recommended statuses:

- `queued`
- `running`
- `completed`
- `failed`
- optional later: `cancelled`

Minimum behavior:

- `POST /reference-runs` creates the run row in `queued`
- one local worker loop claims queued runs and marks them `running`
- worker writes artifacts under `artifact_root`
- worker updates `candidate_count` on completion
- worker records `error_message` on failure
- frontend polls run status
- restart behavior must tolerate a previously `running` run and mark or retry it deterministically

Generalized platform-wide job infrastructure is not required for Phase 1, but this narrower contract is.

## Migration Plan

The current repo uses manual `PRAGMA user_version` migrations.

Add a new data version for Reference Picker.

### Migration Steps

1. Create `referencepickerrun` table.
2. Create `referencevariant` table.
3. Create a new `referenceasset` shape.
4. Migrate old reference assets.

### Old Asset Migration

For each old row:

1. load the referenced `AudioVariant`
2. resolve its parent `Slice`
3. resolve the `SourceRecording`
4. create a new logical `ReferenceAsset`
5. copy the old referenced variant media into a new managed `ReferenceVariant`
6. set the new `active_variant_id`
7. preserve the old asset `id`, `name`, and `created_at`

Do not keep old assets depending on slice variant files after migration. Once the new schema exists, references should own their own managed media.

### Migration Failure Policy

Legacy data may be incomplete.

If migration encounters:

- missing `AudioVariant` row
- missing variant media file
- missing parent `Slice`
- missing `SourceRecording`

the migration should not crash the entire database upgrade by default.

Recommended policy:

- create an archived placeholder asset only if enough information exists to preserve the old identity safely
- otherwise skip the broken legacy asset and emit a structured warning for manual review

Do not silently invent fake provenance.

## Cleanup Rules

Reference assets must change cleanup behavior.

### New Rules

- project media cleanup for slices must not delete managed reference variants
- deleting a picker run must not delete promoted reference media
- archiving a reference asset must not delete its variants immediately
- deleting a reference asset may soft-delete or hard-delete depending on later policy, but must be explicit

### Repo Impact

Current cleanup logic protects slice `AudioVariant` ids referenced by `ReferenceAsset`. Once references own their own media, that protection logic must be updated.

## Security And Validation

The backend must continue validating:

- audio paths on ingest / copy
- managed media path confinement
- sample rate and frame count consistency

Additional validation for references:

- trim bounds must be finite and inside source duration
- promoted asset duration must be positive and above a tiny minimum
- `project_id` and source entities must line up
- rerank anchors must belong to the same project or run scope

## Testing Plan

### Backend

Add tests for:

- listing source recordings
- creating a run
- completing a run and reading its manifest
- reranking with positive and negative anchors
- lazy preview materialization
- promoting a candidate to a reference asset
- creating a reference asset from the current slice state
- running a reference asset processing model
- activating reference variants
- cleanup preserving reference media
- migration from old `ReferenceAsset`

### Frontend

Add tests for:

- route renders and loads source recordings
- run creation form state
- candidate list filtering
- like / dislike rerank interactions
- trim suggestion application
- promote action
- saved reference library refresh
- save-from-label action

### Manual QA

Manual QA scenarios should include:

- long recording with many candidates
- multiple assets saved from the same source variant
- candidate promoted, run deleted, asset still valid
- reference processed twice, variant switching works
- no source recordings in project
- source recording exists but preview extraction fails

## Recommended File-Level Change Map

## Frontend

- `frontend/src/App.tsx`
  - add `reference` step
  - header copy for the new route
- `frontend/src/api.ts`
  - add source-recording, run, candidate, and reference-asset APIs
- `frontend/src/types.ts`
  - add run, candidate, asset, and reference-variant types
- `frontend/src/pages/ReferencePage.tsx`
  - main workstation
- `frontend/src/pages/LabelPage.tsx`
  - library handoff refresh after `Save Current Slice State`
- `frontend/src/workspace/InspectorPane.tsx`
  - add `Save Current Slice State` affordance

## Backend

- `backend/app/models.py`
  - add new tables and request / view models
- `backend/app/main.py`
  - add source-recording list, run, candidate, and reference routes
- `backend/app/repository.py`
  - implement run creation, artifact reads, promotion, reference processing, migration, cleanup changes

## Primary Tensions To Resolve Before Coding

### 1. Dedicated Route Versus Cramming Into Label

Recommendation:

- dedicated `reference` route

Risk if ignored:

- label workstation becomes two products at once

### 2. Minimal ReferenceAsset Patch Versus Real Asset Model

Recommendation:

- real `ReferenceAsset + ReferenceVariant`

Risk if ignored:

- raw-source promotion, subspan saving, and variant processing all become awkward or wrong

### 3. Exact Clip Lab Reuse Versus Reference-Specific Lab

Recommendation:

- shared visual language and extracted components
- separate page state and reference-specific backend endpoints

Risk if ignored:

- trying to force slice-only code to represent references will either pollute `Slice` or create fake slices

### 4. Sync Run Execution Versus Async Run Execution

Recommendation:

- async run model

Risk if ignored:

- long recordings will hang request flow and make the page feel broken

### 5. Candidate Rows In SQLite Versus Artifact Manifests

Recommendation:

- run metadata in DB, candidates on disk

Risk if ignored:

- SQLite becomes a dumping ground for vector-heavy ephemeral data

### 6. One Embedding Versus Separate Similarity Views

Recommendation:

- at minimum: one retrieval embedding plus separate speaker-purity metrics
- later: split speaker and style embeddings more explicitly

Risk if ignored:

- cluster and rerank quality will overfit to nuisance axes

### 7. Frontend Auto-Trim Versus Backend Canonical Trim Service

Recommendation:

- frontend auto-trim first

Risk if ignored:

- trim UX becomes slower and more expensive than needed

## Granular Implementation Risks In This Repo Today

- The frontend has no source-recording list UI yet.
- The app shell project refresh story already drifts; a new reference library will make that drift more visible.
- The current "Clip Lab" is not its own module. It is spread across `EditorPane.tsx`, `InspectorPane.tsx`, slice APIs, and variant history behavior.
- The media system is slice-centric and variant-centric. Reference media adds a second media family.
- Cleanup behavior currently assumes references protect slice variants, not that references own their own variants.
- The repo still lacks a reusable background job substrate, so picker runs may be the second place where long synchronous work hurts.

## Recommended Implementation Order

1. Expand the schema and migration for `ReferencePickerRun`, `ReferenceAsset`, and `ReferenceVariant`.
2. Add source-recording list endpoint and frontend project source selection.
3. Add run creation and run listing endpoints.
4. Implement artifact layout, candidate manifest loading, and candidate preview route.
5. Build the basic Reference page with ranked candidate browse.
6. Add positive / negative rerank loop.
7. Add browser-side auto-trim and candidate promotion.
8. Add reference library listing and detail.
9. Add reference variant processing and active variant switching.
10. Add `Save Current Slice State` from the label workstation.

## One-Line Summary

The correct shape for this feature in the current repo is: a dedicated reference workstation backed by project-scoped discovery runs, candidate artifacts on disk, a real saved `ReferenceAsset + ReferenceVariant` model, browser-side trim refinement, and a reference-specific lab surface that shares Clip Lab ideas without forcing reference work through the `Slice` abstraction.
