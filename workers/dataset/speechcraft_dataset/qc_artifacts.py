from __future__ import annotations

import shutil
from pathlib import Path

from .io import resolve_under_root


QC_SCORE_FILES = (
    "artifacts/transcript_qc.json",
    "artifacts/transcript_qc_summary.json",
    "artifacts/speaker_purity.json",
    "artifacts/speaker_purity_summary.json",
    "artifacts/target_voiceprint.json",
)
DATASET_QC_FILES = (
    "artifacts/dataset_qc.json",
    "artifacts/dataset_qc_summary.json",
)
EXPORT_FILES = (
    "artifacts/export_manifest.json",
    "artifacts/export_audit.json",
    "artifacts/export_summary.json",
)
TEMP_DIRS = ("artifacts/_speaker_purity_stage",)
EXPORT_DIRS = ("artifacts/native_export_clips",)


def _remove_files(run_root: Path, relative_paths: tuple[str, ...]) -> None:
    for relative in relative_paths:
        path = resolve_under_root(run_root, relative)
        if path.exists():
            path.unlink()


def _remove_dirs(run_root: Path, relative_paths: tuple[str, ...]) -> None:
    for relative in relative_paths:
        path = resolve_under_root(run_root, relative)
        if path.exists():
            shutil.rmtree(path)


def clear_qc_score_artifacts(run_root: Path) -> None:
    _remove_files(run_root, QC_SCORE_FILES)
    _remove_dirs(run_root, TEMP_DIRS)


def clear_dataset_qc_artifacts(run_root: Path) -> None:
    _remove_files(run_root, DATASET_QC_FILES)


def clear_export_artifacts(run_root: Path) -> None:
    _remove_files(run_root, EXPORT_FILES)
    _remove_dirs(run_root, EXPORT_DIRS)


def clear_downstream_after_candidate_regeneration(run_root: Path) -> None:
    clear_qc_score_artifacts(run_root)
    clear_dataset_qc_artifacts(run_root)
    clear_export_artifacts(run_root)
