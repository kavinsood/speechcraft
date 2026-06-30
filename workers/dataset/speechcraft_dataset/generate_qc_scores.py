from __future__ import annotations

import argparse
from pathlib import Path

from .io import read_json, resolve_under_root, write_json
from .qc_artifacts import clear_dataset_qc_artifacts, clear_export_artifacts, clear_qc_score_artifacts
from .qc_score_stages import run_speaker_purity_stage, run_transcript_qc_stage
from .run import config_hash, log_line, utc_now_iso, write_status


def _assert_qc_generation_ready(run_root: Path) -> None:
    required = (
        "artifacts/candidate_review_manifest.json",
        "artifacts/speaker_selection.json",
        "artifacts/speaker_regions.jsonl",
        "artifacts/audio_variants_manifest.json",
    )
    missing = [relative for relative in required if not resolve_under_root(run_root, relative).exists()]
    if missing:
        raise ValueError(f"Dataset run is not QC-score-ready; missing: {', '.join(missing)}")


def _assert_qc_generation_allowed(run_root: Path, *, force: bool) -> None:
    if resolve_under_root(run_root, "artifacts/dataset_qc.json").exists() and not force:
        raise ValueError("dataset_qc_already_finalized")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate transcript and speaker QC artifacts for an existing dataset run")
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow regenerating QC scores when dataset_qc.json already exists",
    )
    args = parser.parse_args(argv)
    run_root = Path(args.run_root).expanduser().resolve()
    try:
        config = read_json(Path(args.config).expanduser().resolve())
        config["config_hash"] = config_hash(config)
        write_json(run_root / "config.json", config)
        _assert_qc_generation_ready(run_root)
        _assert_qc_generation_allowed(run_root, force=bool(args.force))
        clear_qc_score_artifacts(run_root)
        clear_dataset_qc_artifacts(run_root)
        clear_export_artifacts(run_root)
        write_status(run_root, {"ok": None, "stage": "transcript_qc", "started_at": utc_now_iso()})
        transcript_qc_summary = run_transcript_qc_stage(run_root, config)
        log_line(run_root, f"transcript_qc generation completed summary={transcript_qc_summary}")
        write_status(run_root, {"ok": None, "stage": "speaker_purity", "summary": transcript_qc_summary})
        speaker_purity_summary = run_speaker_purity_stage(run_root, config)
        log_line(run_root, f"speaker_purity generation completed summary={speaker_purity_summary}")
        write_status(
            run_root,
            {
                "ok": True,
                "stage": "speaker_purity",
                "summary": speaker_purity_summary,
                "config_hash": config["config_hash"],
                "completed_at": utc_now_iso(),
            },
        )
        return 0
    except Exception as exc:
        write_status(
            run_root,
            {
                "ok": False,
                "stage": "speaker_purity",
                "error": f"{type(exc).__name__}: {exc}",
                "reason_codes": ["dataset_qc_score_generation_failed"],
                "completed_at": utc_now_iso(),
            },
        )
        log_line(run_root, f"dataset qc score generation failed: {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
