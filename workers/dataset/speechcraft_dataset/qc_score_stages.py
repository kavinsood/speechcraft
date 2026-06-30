from __future__ import annotations

from pathlib import Path
from typing import Any


def run_transcript_qc_stage(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    from .analyze_ctc_transcript_qc import run_transcript_qc

    return run_transcript_qc(run_root, config)


def run_speaker_purity_stage(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    from .eval_speaker_purity import run_speaker_purity

    return run_speaker_purity(run_root, config)
