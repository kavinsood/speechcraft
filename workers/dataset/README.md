# SpeechCraft Dataset Worker

This worker owns the heavy audio dataset pipeline runtime. The FastAPI backend
starts it as a subprocess and reads `status.json`, logs, summaries, and artifact
files from the run root. The backend must not import this package.

Runtime boundary:

- `backend/`: light API, database, run orchestration, artifact indexing.
- `workers/dataset/`: CUDA/audio stack for VAD, NeMo, ASR, MFA glue, alignment
  QC, SafeCutPoints, candidate assembly, and native-rate export.
- MFA is configured as an external binary through `SPEECHCRAFT_MFA_BIN`.
- MFA's model/temp root can be configured through `SPEECHCRAFT_MFA_ROOT_DIR`.
- MFA runs with `--single_speaker` by default for processing-buffer WAVs.

Initial check:

```bash
cd workers/dataset
uv sync
uv run python scripts/preflight.py --json --artifact-root /tmp/speechcraft-dataset-preflight
```

The script prints a single JSON object suitable for backend/UI consumption.

ASR model setup/check:

```bash
cd workers/dataset
uv run python -m speechcraft_dataset.models download-asr --model tiny.en --json
uv run python -m speechcraft_dataset.models check-asr --model tiny.en --load-model --json
```

For fully local/offline runs, pass a converted faster-whisper model directory in
the worker config:

```json
{
  "faster_whisper_model_path": "/absolute/path/to/local/faster-whisper-model"
}
```

The pipeline should not rely on surprise model downloads during ASR execution.
Preflight reports local ASR model availability, and ASR has bounded model-load
and transcription timeouts.

First CLI path:

```bash
cd workers/dataset
uv run python -m speechcraft_dataset.run \
  --run-root /tmp/speechcraft-dataset-run \
  --source-wav /path/to/source.wav \
  --single-speaker
```

Repeat `--source-wav` for multi-WAV datasets.

Current Phase 2 scope is source preparation, mono 16 kHz analysis-audio
materialization, Silero VAD, single-speaker processing buffers, ASR queue,
faster-whisper ASR, transcript normalization, MFA alignment, alignment QC, and
SafeCutPoint diagnostics:

```text
source_audio -> audio_variants -> vad -> buffers -> asr_queue -> asr -> normalization -> mfa -> alignment_qc -> safe_cutpoints -> candidate_review_clips -> native_export
```

Use `--stop-after audio_variants` when checking the run-root/audio contract
without the heavy Silero/PyTorch worker environment.

Use `--stop-after buffers` to generate padded ASR/MFA processing buffers after
real VAD succeeds. Use `--stop-after normalization` to continue through ASR and
MFA-ready transcript normalization. Use `--stop-after mfa` to build the MFA
corpus, run external MFA, parse TextGrids, and write `aligned_words.jsonl`.
Use `--stop-after alignment_qc` to validate aligned-word timing sanity before
SafeCutPoint generation.
Use `--stop-after safe_cutpoints` to write accepted and rejected cutpoint
diagnostics. This stage does not assemble or export candidate WAV clips.
Use `--stop-after candidate_review_clips` to greedily assemble 3-15 second
review WAVs targeting 8 seconds. These are review artifacts, not final training
exports.
Use `--stop-after native_export` to cut the candidate clips from the original
source WAV sample rate using the analysis-to-native sample mapping. Native
exports write `export_manifest.json`, `export_audit.json`, `export_summary.json`,
and `native_export_clips/*.wav`.

The ASR stage intentionally does not use a faster-whisper `initial_prompt`; the
tracer found prompt echo contamination. Number/symbol handling is done by the
normalization hazard tracker instead.

NeMo, speaker-purity QC, dataset QC, review-decision persistence, and VoxCPM
manifest export remain after this stage contract is stable.

## Offline CTC transcript QC experiment

Standalone tool for manually validating whether Wav2Vec2 CTC confidence ranks
candidate review clips by transcript/audio agreement. This does not change the
production pipeline or frontend.

```bash
cd workers/dataset
uv sync --locked

PYTHONPATH=workers/dataset .venv/bin/python \
  -m speechcraft_dataset.analyze_ctc_transcript_qc \
  --run-root ../../backend/data/media/dataset-runs/mb-mq3wkz25/dataset-17d22e1684db \
  --out /tmp/ctc-qc-madison \
  --export-worst 50
```

Outputs under `--out`:

- `ctc_transcript_qc.json`
- `ctc_transcript_qc_summary.json`
- `ctc_transcript_qc_by_score.csv`
- `worst_clips/` and `best_clips/` (wav + sidecar txt for ear-checking)

If `import ctc_segmentation` fails with a NumPy 2 ABI error after install,
rebuild the extension from source against the current NumPy:

```bash
cd /tmp && curl -sL https://files.pythonhosted.org/packages/source/c/ctc_segmentation/ctc_segmentation-1.7.4.tar.gz -o ctc_segmentation-1.7.4.tar.gz
tar xf ctc_segmentation-1.7.4.tar.gz && cd ctc_segmentation-1.7.4
NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION ../workers/dataset/.venv/bin/python setup.py build_ext --inplace
cp ctc_segmentation/ctc_segmentation_dyn*.so ../workers/dataset/.venv/lib/python3.12/site-packages/ctc_segmentation/
```

## Backend Run Lifecycle

The FastAPI backend now owns the coarse `ProcessingRun` lifecycle without
importing this package:

```text
POST /api/projects/{project_id}/dataset-runs
POST /api/dataset-runs/{run_id}/start
POST /api/dataset-runs/{run_id}/refresh
GET  /api/dataset-runs/{run_id}
GET  /api/dataset-runs/{run_id}/log
```

Runs live under the backend storage root at
`dataset-runs/{project_id}/{run_id}`. Refreshing a run reads `status.json`,
indexes known file-backed artifacts with hashes, and fails closed if the worker
exits without writing status.
