# Dataset Worker Phase Review Bundle

## Scope

Current staged dataset-worker path:

```text
source_audio -> audio_variants -> vad -> buffers -> asr_queue -> asr -> normalization -> mfa -> alignment_qc -> safe_cutpoints -> candidate_review_clips
```

The backend remains light. Heavy audio/ML code lives under `workers/dataset`.

## Verified Behavior

```text
source WAV inspection and hashing
multi-WAV source manifest
mono 16 kHz analysis WAV materialization
native/source -> analysis coordinate metadata
Silero VAD lazy import
VAD segment JSONL with integer analysis sample indices
VAD model/tensor cleanup and torch.cuda.empty_cache()
single-speaker processing buffer generation
NumPy/soundfile buffer audio IO
progressive VAD-gap split search with RMS fallback
no-speech default produces zero buffers and no_speech_detected
padded buffer WAV extraction
processing_buffers.json ledger with source/trusted/local sample coordinates
ASR/MFA queue with <5s rejection
ASR model setup/check command for faster-whisper local cache/path validation
preflight ASR model availability reporting
bounded ASR model-load and transcription timeouts
faster-whisper ASR without initial_prompt
empty ASR queue short-circuits without importing/loading faster-whisper
ASR language/task/VAD/conditioning options are config-backed
normalization preserving raw/training text and producing alignment_text
numeric/symbol hazard tracking
computed config_hash in config.json and stage summaries
MFA corpus generation from normalized transcripts and ASR/MFA queue audio
external MFA subprocess invocation with configured dictionary/acoustic model/root
MFA --single_speaker enabled by default for processing-buffer WAVs
consistent mfa_summary.json even when MFA corpus is empty
configured MFA binary path validation
expected/missing/unexpected TextGrid accounting
run-local OOV artifact preference with fallback-root diagnostics
MFA TextGrid parsing through praatio
MFA OOV artifact discovery/snapshot and oov_words.json
aligned_words.jsonl with source/local integer sample indices
hazard/OOV propagation onto aligned words
alignment_qc_by_buffer.json and alignment_qc_summary.json
alignment QC for token/word mismatches, impossible durations, backwards order, boundary exceedance, trusted-edge warnings, OOV/symbol/numeric hazards, and ASR confidence fields
alignment QC separates warning_reason_codes from fatal_reason_codes and explicitly disables automatic cutpoints for fatal buffers
alignment QC accounts for aligned-word rows referencing unexpected/stale buffer IDs
alignment QC summaries do not self-hash their own content
SafeCutPoint diagnostics use integer sample indices and MFA word gaps as boundary authority
SafeCutPoint diagnostics apply RMS valley/noise-floor math plus alignment-QC fatal gates and provisional/OOV/symbol/numeric guards
SafeCutPoint diagnostics fail closed when a buffer has no alignment-QC row and account for unexpected/stale QC buffer rows
accepted SafeCutPoints carry nonfatal buffer warning context
safe_cutpoints.jsonl, rejected_cutpoint_candidates.jsonl, and safe_cutpoint_summary.json
candidate review clip assembly uses only accepted SafeCutPoint pairs within one processing buffer
candidate review WAVs use exact integer buffer-local samples and carry source/global sample provenance
candidate review manifests reconstruct training text from raw token IDs and propagate OOV/symbol/numeric/QC warnings
stage status.json failure reason codes
stage summaries with input/output artifact hashes
```

## Test Results

Ambient worker tests:

```bash
PYTHONPATH=workers/dataset python -m unittest discover -s workers/dataset/tests
```

```text
Ran 30 tests
OK (skipped=12)
```

The skipped tests require worker audio dependencies such as NumPy/soundfile.

Dataset worker venv tests:

```bash
PYTHONPATH=workers/dataset workers/dataset/.venv/bin/python -m unittest discover -s workers/dataset/tests
```

```text
Ran 30 tests
OK
```

Backend focused tests:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest \
  tests.test_rfc_artifact_spine \
  tests.test_dataset_worker_client \
  tests.test_cors_config
```

```text
Ran 17 tests
OK
```

## Real Worker-Env Preflight

Preflight from `workers/dataset/.venv/bin/python`:

```text
torch: 2.11.0+cu128 ok
torchaudio: 2.11.0+cu128 ok
nemo: 2.7.3 ok
faster_whisper: 1.2.1 ok
ctranslate2: 4.7.2 ok
silero_vad: 6.2.1 ok
soundfile: 0.13.1 ok
praatio: ok
numpy: 2.4.6 ok
scipy: 1.17.1 ok
ffmpeg: ok
MFA: not configured/on PATH
CUDA available: false in this run
overall ok: false because MFA is not configured
```

## Real Smokes

Real speech VAD/buffers smoke:

```bash
PYTHONPATH=workers/dataset workers/dataset/.venv/bin/python \
  -m speechcraft_dataset.run \
  --run-root <tmp>/run \
  --source-wav /home/aaravthegreat/Projects/Charactors/HeyBillieRae/good_raw/clip17Billie_[cut_9sec].wav \
  --single-speaker \
  --stop-after buffers
```

Result:

```text
ok: true
stage: buffers
vad segment_count: 3
vad speech_duration_sec: 7.95375
buffer_count: 1
buffers_under_max: true
split_strategy_counts: {"whole_region": 1}
```

No-speech sine-tone smoke:

```bash
PYTHONPATH=workers/dataset workers/dataset/.venv/bin/python \
  -m speechcraft_dataset.run \
  --run-root <tmp>/run \
  --source-wav <generated-tone.wav> \
  --single-speaker \
  --stop-after buffers
```

Result:

```text
ok: true
stage: buffers
buffer_count: 0
skipped_source_count: 1
reason_codes: ["no_speech_detected"]
allow_no_vad_full_span_fallback: false
```

ASR smoke:

```bash
PYTHONPATH=workers/dataset workers/dataset/.venv/bin/python \
  -m speechcraft_dataset.run \
  --run-root <tmp>/run \
  --source-wav /home/aaravthegreat/Projects/Charactors/HeyBillieRae/good_raw/clip17Billie_[cut_9sec].wav \
  --single-speaker \
  --stop-after normalization \
  --config <tiny-en-cpu-config.json>
```

Result:

```text
ok: true
stage: normalization
asr_queue ready_buffers: 1
asr_queue ready_duration_sec: 9.15375
ASR model: tiny.en
ASR device/compute_type: cpu/int8
ASR empty_transcripts: 0
ASR total_chars: 160
normalization total_tokens: 30
normalization empty_normalized_transcripts: 0
numeric/symbol review hazards: 0
```

Real MFA smoke:

```bash
PATH=/home/aaravthegreat/.conda/envs/speechcraft-mfa/bin:$PATH \
PYTHONPATH=workers/dataset \
SPEECHCRAFT_MFA_BIN=/home/aaravthegreat/.conda/envs/speechcraft-mfa/bin/mfa \
SPEECHCRAFT_MFA_DICTIONARY=english_us_mfa \
SPEECHCRAFT_MFA_ACOUSTIC_MODEL=english_mfa \
workers/dataset/.venv/bin/python -m speechcraft_dataset.run \
  --run-root /tmp/speechcraft-mfa-smoke-B6W5CN/run \
  --source-wav /home/aaravthegreat/Projects/Charactors/HeyBillieRae/good_raw/clip17Billie_[cut_9sec].wav \
  --single-speaker \
  --stop-after mfa \
  --config <tiny-en-cpu-mfa-config.json>
```

Result:

```text
ok: true
stage: mfa
mfa version: 3.3.9
mfa_summary.status: ok
mfa command included --single_speaker
expected_textgrid_count: 1
textgrid_count: 1
missing_textgrid_count: 0
unexpected_textgrid_count: 0
aligned_word_count: 30
buffers_with_alignment_token_word_mismatch: 0
oov_word_count: 0
numeric_oov_word_count: 0
TextGrid: /tmp/speechcraft-mfa-smoke-B6W5CN/run/artifacts/mfa_output/buffer_000000.TextGrid
```

Real alignment QC smoke:

```bash
PATH=/home/aaravthegreat/.conda/envs/speechcraft-mfa/bin:$PATH \
PYTHONPATH=workers/dataset \
SPEECHCRAFT_MFA_BIN=/home/aaravthegreat/.conda/envs/speechcraft-mfa/bin/mfa \
SPEECHCRAFT_MFA_DICTIONARY=english_us_mfa \
SPEECHCRAFT_MFA_ACOUSTIC_MODEL=english_mfa \
workers/dataset/.venv/bin/python -m speechcraft_dataset.run \
  --run-root /tmp/speechcraft-alignment-qc-smoke-4nAvSu/run \
  --source-wav /home/aaravthegreat/Projects/Charactors/HeyBillieRae/good_raw/clip17Billie_[cut_9sec].wav \
  --single-speaker \
  --stop-after alignment_qc \
  --config <tiny-en-cpu-mfa-config.json>
```

Result:

```text
ok: true
stage: alignment_qc
buffer_count: 1
aligned_word_count: 30
normalized_token_count: 30
buffers_with_no_words: 0
buffers_with_alignment_mismatch: 0
buffers_with_alignment_token_word_mismatch: 0
buffers_with_words_outside_buffer: 0
buffers_with_words_outside_trusted_chunk: 0
buffers_with_non_positive_word_durations: 0
buffers_with_absurdly_short_words: 0
buffers_with_absurdly_long_words: 0
buffers_with_backwards_word_order: 0
buffers_with_oovs: 0
p50_word_duration_sec: 0.26
p90_word_duration_sec: 0.4
p50_word_gap_sec: 0.0
p90_word_gap_sec: 0.006
by_buffer reason_codes: ["word_near_trusted_edge"]
words_near_trusted_edges: 2
```

Real SafeCutPoint diagnostics smoke:

```bash
PATH=/home/aaravthegreat/.conda/envs/speechcraft-mfa/bin:$PATH \
PYTHONPATH=workers/dataset \
SPEECHCRAFT_MFA_BIN=/home/aaravthegreat/.conda/envs/speechcraft-mfa/bin/mfa \
SPEECHCRAFT_MFA_DICTIONARY=english_us_mfa \
SPEECHCRAFT_MFA_ACOUSTIC_MODEL=english_mfa \
workers/dataset/.venv/bin/python -m speechcraft_dataset.run \
  --run-root /tmp/speechcraft-safecut-smoke-ZQTr9U/run \
  --source-wav /home/aaravthegreat/Projects/Charactors/HeyBillieRae/good_raw/clip17Billie_[cut_9sec].wav \
  --single-speaker \
  --stop-after safe_cutpoints \
  --config <tiny-en-cpu-mfa-config.json>
```

Result:

```text
ok: true
stage: safe_cutpoints
buffers_evaluated: 1
buffers_with_automatic_cutpoints_disabled: 0
accepted_cutpoints: 2
rejected_cutpoint_candidates: 27
acceptance_rate: 0.0689655
p50_gap_duration_sec: 0.66
p90_gap_duration_sec: 0.972
p50_valley_dbfs: -93.784599
p90_valley_dbfs: -85.103831
rejection_reason_counts:
  non_positive_word_gap: 26
  usable_gap_too_short: 27
```

The two accepted cuts were integer analysis-source sample indices with empty
reason-code lists. No candidate clips or WAVs were assembled by this stage.

Real candidate review clip smoke:

```bash
PATH=/home/aaravthegreat/.conda/envs/speechcraft-mfa/bin:$PATH \
PYTHONPATH=workers/dataset \
SPEECHCRAFT_MFA_BIN=/home/aaravthegreat/.conda/envs/speechcraft-mfa/bin/mfa \
SPEECHCRAFT_MFA_DICTIONARY=english_us_mfa \
SPEECHCRAFT_MFA_ACOUSTIC_MODEL=english_mfa \
workers/dataset/.venv/bin/python -m speechcraft_dataset.run \
  --run-root /tmp/speechcraft-candidate-smoke-BdD54B/run \
  --source-wav /home/aaravthegreat/Projects/Charactors/HeyBillieRae/good_raw/clip9Billie_[cut_19sec].wav \
  --single-speaker \
  --stop-after candidate_review_clips \
  --config <tiny-en-cpu-mfa-config.json>
```

Result after correcting buffer-warning versus clip-local review propagation:

```text
ok: true
stage: candidate_review_clips
candidate_review_clips: 2
total_duration_sec: 13.44
min_clip_duration_sec: 4.1
max_clip_duration_sec: 9.34
rejected_spans: 0
clips_needing_review: 0
all clips have start/end SafeCutPoint refs
all clips stay inside one processing buffer
actual WAVs:
  artifacts/candidate_review_clips/candidate_review_clip_000000.wav
  artifacts/candidate_review_clips/candidate_review_clip_000001.wav
```

Buffer-level warning context remains in `buffer_warning_reason_codes`, but only
hazards inside a clip contribute to `needs_review` and
`review_reason_codes`.

Notes:

```text
The first sandboxed attempt failed because MFA could not write to its normal ~/Documents/MFA working directory.
A second attempt with a fresh MFA_ROOT_DIR failed because installed MFA models live in the default MFA model store.
The successful smoke used the conda env bin directory on PATH and the default MFA model store.
```

ASR model setup/check added after smoke failure:

```bash
PYTHONPATH=workers/dataset workers/dataset/.venv/bin/python \
  -m speechcraft_dataset.models check-asr --model tiny.en --json

PYTHONPATH=workers/dataset workers/dataset/.venv/bin/python \
  -m speechcraft_dataset.models download-asr --model tiny.en --json
```

The worker config now also supports:

```json
{
  "faster_whisper_model_path": "/absolute/path/to/local/faster-whisper-model",
  "asr_model_load_timeout_sec": 180,
  "asr_transcribe_timeout_sec": 600
}
```

## Remaining Known Gaps

```text
CUDA was not available during this preflight run.
NeMo, speaker-purity QC, dataset QC, and native-rate export are not ported yet.
```

## Backend ProcessingRun Integration

The backend can now create, start, refresh, list, and inspect dataset runs while
remaining dependency-light. It launches the explicit dataset-worker Python,
captures bounded logs, reads worker `status.json`, and indexes known artifacts
as `RunArtifact` rows with relative paths, byte sizes, and SHA-256 hashes.

Guardrails:

```text
backend-controlled source/config keys cannot be overridden by user config
multi-speaker runs are rejected until NeMo is implemented
only pending runs can start
dead workers without status fail closed
dead workers with partial non-terminal status fail closed
manifest payloads are not copied into RunArtifact.summary
candidate_review_rejected.json is indexed
artifact paths resolve under the configured storage root
```

Verification:

```text
backend focused tests: 28 OK
dataset worker tests: 30 OK
candidate assembly invariant test covers SafeCutPoint refs, sample provenance,
word containment, duration bounds, and WAV sample count
```

## Functional Processing UI

The workflow now includes a `Processing` page between Overview and Slicer.
Ingest and Overview preparation controls remain unchanged.

The page provides:

```text
multi-WAV source selection
dataset-worker preflight status
all current VAD / processing-buffer / ASR / MFA / alignment-QC controls
visible disabled NeMo controls until multi-speaker support exists
diagnostic stop-after selection
create-and-start lifecycle
run history and stage/status display
two-second active-run polling
bounded raw worker terminal output
indexed artifact list
```

Preflight runs independently so a slow dependency check does not block source
selection or run history. Launching is disabled while a fresh preflight is
running, so a stale green result cannot start a worker.

Pending-run lifecycle is recoverable:

```text
create succeeds + start fails -> pending run remains selected
selected pending run -> Start selected pending run action
running run -> blocks another run
completed/failed run -> permits a new run
```

Editable ASR controls are consumed by the worker. Required pipeline rules such
as MFA cut authority, disabled Whisper word timestamps, and disabled
previous-text conditioning remain visible but disabled.

Frontend verification:

```text
production build: passed
full frontend tests: 19 passed
```
