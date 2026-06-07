from __future__ import annotations

import argparse
from pathlib import Path

from .assembly import assemble_candidate_review_clips
from .io import read_json, write_json
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
        write_status(run_root, {"ok": None, "stage": "safe_cutpoints", "started_at": utc_now_iso()})
        safe_summary = generate_safe_cutpoint_diagnostics(run_root, config)
        log_line(run_root, f"safe_cutpoints rerun completed summary={safe_summary}")
        write_status(run_root, {"ok": None, "stage": "candidate_review_clips", "summary": safe_summary})
        candidate_summary = assemble_candidate_review_clips(run_root, config)
        log_line(run_root, f"candidate_review_clips rerun completed summary={candidate_summary}")
        write_status(run_root, {"ok": True, "stage": "candidate_review_clips", "summary": candidate_summary, "config_hash": config["config_hash"], "completed_at": utc_now_iso()})
        return 0
    except Exception as exc:
        write_status(run_root, {"ok": False, "stage": "candidate_review_clips", "error": f"{type(exc).__name__}: {exc}", "reason_codes": ["dataset_slicer_rerun_failed"], "completed_at": utc_now_iso()})
        log_line(run_root, f"dataset slicer rerun failed: {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
