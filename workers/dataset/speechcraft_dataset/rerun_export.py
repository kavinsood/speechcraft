from __future__ import annotations

import argparse
from pathlib import Path

from .export import export_native_candidate_clips
from .io import read_json, write_json
from .run import config_hash, log_line, utc_now_iso, write_status


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rerun SpeechCraft native-rate export")
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    run_root = Path(args.run_root).expanduser().resolve()
    try:
        config = read_json(Path(args.config).expanduser().resolve())
        config["config_hash"] = config_hash(config)
        write_json(run_root / "config.json", config)
        write_status(run_root, {"ok": None, "stage": "native_export", "started_at": utc_now_iso()})
        export_summary = export_native_candidate_clips(run_root, config)
        log_line(run_root, f"native_export rerun completed summary={export_summary}")
        write_status(
            run_root,
            {
                "ok": True,
                "stage": "native_export",
                "summary": export_summary,
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
                "stage": "native_export",
                "error": f"{type(exc).__name__}: {exc}",
                "reason_codes": ["dataset_export_rerun_failed"],
                "completed_at": utc_now_iso(),
            },
        )
        log_line(run_root, f"dataset export rerun failed: {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
