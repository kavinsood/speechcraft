from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from sqlmodel import select

from app.models import Slice, SourceRecording, Transcript
from app.repository import SQLiteRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Quick-export a temp batch using manual accepts plus unflagged unresolved clips."
    )
    parser.add_argument("--batch-id", required=True, help="ImportBatch id to export from.")
    parser.add_argument(
        "--output-dir",
        help="Directory to write the export into. Defaults to backend/exports/quick-export/<batch-id>.",
    )
    return parser.parse_args()


def transcript_text(transcript: Transcript | None) -> str:
    if transcript is None:
        return ""
    return (transcript.modified_text or transcript.original_text or "").strip()


def main() -> None:
    args = parse_args()
    repository = SQLiteRepository()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    output_root = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else (repository.exports_root / "quick-export" / f"{args.batch_id}-{timestamp}")
    )
    rendered_root = output_root / "rendered"
    manifest_path = output_root / "dataset.list"
    jsonl_path = output_root / "voxcpm_train.jsonl"
    summary_path = output_root / "_summary.json"

    with repository._session() as session:
        recording_ids = session.exec(
            select(SourceRecording.id).where(SourceRecording.batch_id == args.batch_id)
        ).all()
        if not recording_ids:
            raise SystemExit(f"No source recordings found for batch {args.batch_id!r}")

        slices = session.exec(
            select(Slice)
            .where(Slice.source_recording_id.in_(recording_ids))
            .order_by(Slice.created_at, Slice.id)
        ).all()

        selected: list[tuple[Slice, str]] = []
        manual_accept_count = 0
        auto_keep_count = 0
        skipped_rejected = 0
        skipped_flagged = 0
        skipped_empty = 0

        for slice_row in slices:
            transcript = session.exec(
                select(Transcript).where(Transcript.slice_id == slice_row.id)
            ).first()
            text = transcript_text(transcript)
            if not text:
                skipped_empty += 1
                continue

            alignment_data = transcript.alignment_data if transcript is not None else None
            is_flagged = bool((alignment_data or {}).get("is_flagged"))

            if slice_row.status.value == "rejected":
                skipped_rejected += 1
                continue
            if slice_row.status.value == "accepted":
                selected.append((slice_row, text))
                manual_accept_count += 1
                continue
            if not is_flagged:
                selected.append((slice_row, text))
                auto_keep_count += 1
            else:
                skipped_flagged += 1

    rendered_root.mkdir(parents=True, exist_ok=True)
    manifest_lines: list[str] = []
    jsonl_lines: list[str] = []
    rendered_count = 0

    for slice_row, text in selected:
        rendered_path = rendered_root / f"{slice_row.id}.wav"
        rendered_path.write_bytes(repository.get_clip_audio_bytes(slice_row.id))
        speaker = str((slice_row.model_metadata or {}).get("speaker_name") or "speaker_a")
        language = str((slice_row.model_metadata or {}).get("language") or "en")
        manifest_lines.append(f"{rendered_path}|{speaker}|{language}|{text}")
        jsonl_lines.append(
            json.dumps(
                {
                    "audio": str(rendered_path),
                    "text": text,
                },
                ensure_ascii=False,
            )
        )
        rendered_count += 1

    manifest_path.write_text("\n".join(manifest_lines), encoding="utf-8")
    jsonl_path.write_text("\n".join(jsonl_lines), encoding="utf-8")
    summary = {
        "batch_id": args.batch_id,
        "output_root": str(output_root),
        "manifest_path": str(manifest_path),
        "voxcpm_jsonl_path": str(jsonl_path),
        "rendered_count": rendered_count,
        "manual_accepted_kept": manual_accept_count,
        "auto_kept_unflagged_unresolved": auto_keep_count,
        "skipped_rejected": skipped_rejected,
        "skipped_flagged_unresolved": skipped_flagged,
        "skipped_empty_transcript": skipped_empty,
        "selection_policy": "accepted OR (unresolved AND not alignment_data.is_flagged)",
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
