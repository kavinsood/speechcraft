# Dataset Worker Phase 1 Notes

## Dev Database Reset

This branch intentionally renames the Phase 1 audio variant model from
`RfcAudioVariant` to `DatasetAudioVariant`.

The persistent table name is now explicitly fixed as:

```text
datasetaudiovariant
```

No production migration is included because this branch has not been merged to
main and existing Phase 1 databases are disposable development artifacts. Reset
local dev databases after pulling this change.

If any database with real data already contains `rfcaudiovariant`, add an
explicit table/data migration before using that database with this branch.

## Path Rules

`ProcessingRun.artifact_root` is storage-root-relative, for example:

```text
runs/project-1/run-1
```

`RunArtifact.path` is relative under that run root, for example:

```text
artifacts/aligned_words.jsonl
logs/dataset_worker.log
```

Backend code must resolve artifacts through the safe resolver instead of string
concatenation:

```python
resolve_run_artifact_path(storage_root, processing_run.artifact_root, run_artifact.path)
```
