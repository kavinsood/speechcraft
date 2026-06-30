from __future__ import annotations

import argparse
from pathlib import Path

from .clip_lab_coordination import assemble_candidate_review_clips_locked
from .io import read_json, write_json
from .qc_artifacts import clear_downstream_after_candidate_regeneration
from .qc_score_stages import run_speaker_purity_stage, run_transcript_qc_stage
from .run import config_hash, log_line, utc_now_iso, write_status
from .safecut import generate_safe_cutpoint_diagnostics


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rerun SpeechCraft SafeCutPoints and candidate assembly")
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    run_root = Path(args.run_root).expanduser().resolve()
    try:
        config = read_json(Path(args.config).expanduser().resolve())
        config["config_hash"] = config_hash(config)
        write_json(run_root / "config.json", config)
        clear_downstream_after_candidate_regeneration(run_root)
        write_status(run_root, {"ok": None, "stage": "safe_cutpoints", "started_at": utc_now_iso()})
        safe_summary = generate_safe_cutpoint_diagnostics(run_root, config)
        log_line(run_root, f"safe_cutpoints rerun completed summary={safe_summary}")
        write_status(run_root, {"ok": None, "stage": "candidate_review_clips", "summary": safe_summary})
        candidate_summary = assemble_candidate_review_clips_locked(run_root, config)
        log_line(run_root, f"candidate_review_clips rerun completed summary={candidate_summary}")
        try:
            write_status(run_root, {"ok": None, "stage": "transcript_qc", "summary": candidate_summary})
            transcript_qc_summary = run_transcript_qc_stage(run_root, config)
            log_line(run_root, f"transcript_qc rerun completed summary={transcript_qc_summary}")
            write_status(run_root, {"ok": None, "stage": "speaker_purity", "summary": transcript_qc_summary})
            speaker_purity_summary = run_speaker_purity_stage(run_root, config)
            log_line(run_root, f"speaker_purity rerun completed summary={speaker_purity_summary}")
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
        except Exception as qc_exc:  # noqa: BLE001 - preserve regenerated candidate clips on QC failure
            log_line(run_root, f"qc rerun failed after candidate regeneration: {type(qc_exc).__name__}: {qc_exc}")
            write_status(
                run_root,
                {
                    "ok": True,
                    "stage": "candidate_review_clips",
                    "summary": candidate_summary,
                    "config_hash": config["config_hash"],
                    "reason_codes": ["qc_artifacts_failed"],
                    "warning": f"{type(qc_exc).__name__}: {qc_exc}",
                    "completed_at": utc_now_iso(),
                },
            )
        return 0
    except Exception as exc:
        write_status(run_root, {"ok": False, "stage": "speaker_purity", "error": f"{type(exc).__name__}: {exc}", "reason_codes": ["dataset_slicer_rerun_failed"], "completed_at": utc_now_iso()})
        log_line(run_root, f"dataset slicer rerun failed: {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
