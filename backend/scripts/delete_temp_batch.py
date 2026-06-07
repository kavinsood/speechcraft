from __future__ import annotations

import argparse
import json

from sqlmodel import delete, select

from app.models import (
    ExportRun,
    ImportBatch,
    ProcessingJob,
    SourceRecording,
    SourceRecordingArtifact,
)
from app.repository import SQLiteRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delete a temporary Speechcraft project batch and its managed media.")
    parser.add_argument("--batch-id", required=True, help="ImportBatch id to delete.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repository = SQLiteRepository()

    with repository._session() as session:
        batch = session.get(ImportBatch, args.batch_id)
        if batch is None:
            raise SystemExit(f"Batch not found: {args.batch_id}")

        recording_ids = session.exec(select(SourceRecording.id).where(SourceRecording.batch_id == args.batch_id)).all()
        source_paths = (
            session.exec(select(SourceRecording.file_path).where(SourceRecording.id.in_(recording_ids))).all()
            if recording_ids
            else []
        )

        if recording_ids:
            session.exec(delete(SourceRecordingArtifact).where(SourceRecordingArtifact.source_recording_id.in_(recording_ids)))
            session.exec(delete(ProcessingJob).where(ProcessingJob.source_recording_id.in_(recording_ids)))
            session.exec(delete(SourceRecording).where(SourceRecording.id.in_(recording_ids)))
        session.exec(delete(ExportRun).where(ExportRun.batch_id == args.batch_id))
        session.exec(delete(ImportBatch).where(ImportBatch.id == args.batch_id))
        session.commit()

    deleted_file_count = repository._delete_managed_media_paths(list(source_paths))
    deleted_file_count += repository._prune_derived_media_cache()
    print(
        json.dumps(
            {
                "deleted_batch_id": args.batch_id,
                "deleted_managed_files": deleted_file_count,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
