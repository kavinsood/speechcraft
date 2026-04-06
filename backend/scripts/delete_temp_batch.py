from __future__ import annotations

import argparse
import json

from sqlmodel import delete, select

from app.models import (
    AudioVariant,
    DatasetProcessingRun,
    EditCommit,
    ExportRun,
    ImportBatch,
    ProcessingJob,
    ReviewWindow,
    ReviewWindowRevision,
    ReviewWindowVariant,
    Slice,
    SliceTagLink,
    SourceRecording,
    Transcript,
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
        slice_ids = session.exec(select(Slice.id).where(Slice.source_recording_id.in_(recording_ids))).all() if recording_ids else []
        review_window_ids = session.exec(
            select(ReviewWindow.id).where(ReviewWindow.source_recording_id.in_(recording_ids))
        ).all() if recording_ids else []
        deleted_paths = (
            session.exec(select(AudioVariant.file_path).where(AudioVariant.slice_id.in_(slice_ids))).all()
            if slice_ids
            else []
        )

        if slice_ids:
            session.exec(delete(SliceTagLink).where(SliceTagLink.slice_id.in_(slice_ids)))
            session.exec(delete(EditCommit).where(EditCommit.slice_id.in_(slice_ids)))
            session.exec(delete(Transcript).where(Transcript.slice_id.in_(slice_ids)))
            session.exec(delete(AudioVariant).where(AudioVariant.slice_id.in_(slice_ids)))
            session.exec(delete(Slice).where(Slice.id.in_(slice_ids)))
        if review_window_ids:
            session.exec(delete(ReviewWindowRevision).where(ReviewWindowRevision.review_window_id.in_(review_window_ids)))
            session.exec(delete(ReviewWindowVariant).where(ReviewWindowVariant.review_window_id.in_(review_window_ids)))
            session.exec(delete(ReviewWindow).where(ReviewWindow.id.in_(review_window_ids)))
        if recording_ids:
            session.exec(delete(ProcessingJob).where(ProcessingJob.source_recording_id.in_(recording_ids)))
            session.exec(delete(DatasetProcessingRun).where(DatasetProcessingRun.source_recording_id.in_(recording_ids)))
            session.exec(delete(SourceRecording).where(SourceRecording.id.in_(recording_ids)))
        session.exec(delete(ExportRun).where(ExportRun.batch_id == args.batch_id))
        session.exec(delete(ImportBatch).where(ImportBatch.id == args.batch_id))
        session.commit()

    deleted_file_count = repository._delete_unreferenced_media_files(list(deleted_paths))
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
