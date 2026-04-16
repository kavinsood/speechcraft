# Dataset Overview

## What the Overview page is

The **Overview** page is the first real dataset page after ingest.

Its job is to help a user understand the raw recordings they imported, choose preparation settings, and decide whether the dataset is ready to move into slicing.

It is a **recording-centric** page.
It is not a slice review page.
It is not a QC page.
It is not a transcript quality page.

The Overview page answers four basic questions:

1. **What raw audio is in this dataset?**
2. **What basic technical properties does it have?**
3. **What preparation settings will be applied before slicing?**
4. **Is the dataset ready to move into the next stage?**

## What unit of truth it owns

For this sprint, the Overview page owns:
- imported raw recordings
- source-level technical dataset state
- preparation configuration
- preparation job status

It does **not** own:
- slices
- slice-level QC
- clip acceptance/rejection decisions
- machine review buckets
- manual review state

That separation is deliberate.

## What the user sees on the Overview page

## 1. Dataset summary

The page should show the basic technical shape of the imported dataset.

For this sprint, the required summary information is:
- total duration
- number of recordings
- sample rate(s)
- channel count(s)

This is the minimum sane snapshot of the dataset.

The goal is not to pretend to do smart analysis here.
The goal is to give the user a clear source-level inventory.

## 2. Technical warnings

Overview should support simple, honest technical warnings.

For this sprint, the initial warning set is:
- mixed sample rates across imported recordings
- mixed channel counts across imported recordings
- no recordings imported
- preparation settings changed but no prepared derivative has been generated yet

These warnings are practical and deterministic.

Overview should **not** invent a fake “dataset quality score.”
That belongs later, after slicing, on the QC page.

## 3. Preparation controls

Preparation tools live on the Overview page.

For this sprint, the in-scope preparation actions are:
- downsampling
- mono/downmix
- channel selection

Loudness normalization is intentionally deferred for now.

Preparation is a dataset-level source operation.
It exists to create a prepared dataset copy suitable for downstream slicing.

## How preparation works

Preparation is explicit and user-triggered.

The intended interaction is:
- user chooses preparation settings
- user clicks a button to run preparation
- the system creates a new prepared dataset copy on disk
- that prepared output becomes the input scope for slicer runs

Important rule:

**Preparation must not silently mutate raw imported files.**

Original audio stays immutable.
Preparation produces a derived output.

## Job visibility on Overview

Preparation can take time, so the Overview page must visibly show job activity.

For this sprint, preparation jobs must have a visible activity surface showing:
- job type
- running / completed / failed state
- start time
- simple progress state when available
- log output or terminal-style streamed text
- clear completion message
- clear failure message

A spinner alone is not enough.
A toast alone is not enough.
Silent polling is not enough.

The user must never wonder whether the app is frozen.

## What the Overview page is not

The Overview page is not where the user should see:
- slice-level quality analysis
- QC graphs
- clip acceptance/rejection logic
- transcript quality scoring
- speaker analytics

Those belong to later stages.

Overview is about the state of the **raw imported dataset** and the **preparation settings** that will shape the input to slicing.

## What happens after Overview

Once the user has:
- imported recordings
- checked the source-level stats
- reviewed warnings
- applied preparation if needed

then the next intended stage is:

**Slicer**

The prepared dataset becomes the input for slicer runs.

## Why this page exists

Without an Overview page, ingest is just a file drop and the user has no clear idea:
- what was imported
- whether the dataset is technically consistent
- whether preparation is needed
- whether the system is ready to slice

Overview gives the user a clean source-level checkpoint before the workflow becomes slice-based.

## In one sentence

The Overview page is the source-level dataset page where the user inspects imported recordings, sees basic technical warnings, chooses preparation settings, runs preparation jobs, and confirms the dataset is ready for slicing.

