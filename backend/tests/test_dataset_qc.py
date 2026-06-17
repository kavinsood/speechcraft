from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import TestCase

from pydantic import ValidationError
from sqlmodel import Session

from app.dataset_qc import (
    CANDIDATE_MANIFEST_REL,
    DATASET_QC_REL,
    DEFAULT_SPEAKER_THRESHOLD,
    DEFAULT_TRANSCRIPT_THRESHOLD,
    TRANSCRIPT_QC_REL,
    finalize_dataset_qc,
    get_dataset_qc,
)
from app.dataset_runs import create_dataset_run, refresh_dataset_run
from app.models import (
    DatasetQcFinalizeRequest,
    DatasetQcManualOverrideRequest,
    DatasetQcThresholdsRequest,
    DatasetRunCreateRequest,
    ImportBatch,
    RunArtifactKind,
    SourceRecording,
)
from app.repository import SQLiteRepository

FIXTURES_ROOT = Path(__file__).resolve().parent / "fixtures" / "qc"


class DatasetQcTests(TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.repository = SQLiteRepository(
            db_path=root / "project.db",
            legacy_seed_path=root / "missing-seed.json",
            media_root=root / "media",
            exports_root=root / "exports",
        )
        source_path = root / "source.wav"
        source_path.write_bytes(b"RIFF")
        with Session(self.repository.engine) as session:
            session.add(ImportBatch(id="project-1", name="Project 1"))
            session.add(
                SourceRecording(
                    id="recording-1",
                    batch_id="project-1",
                    file_path=str(source_path),
                    sample_rate=48000,
                    num_channels=1,
                    num_samples=48000,
                )
            )
            session.commit()
        self.run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        self.run_root = self.repository.media_root / str(self.run.artifact_root)
        self._install_qc_fixtures(self.run_root, include_qc_artifacts=True)

    def tearDown(self) -> None:
        self.repository.close()
        self.temp_dir.cleanup()

    def _install_qc_fixtures(self, run_root: Path, *, include_qc_artifacts: bool) -> None:
        artifacts = run_root / "artifacts"
        clips_dir = artifacts / "candidate_review_clips"
        clips_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURES_ROOT / "candidate_review_manifest.json", artifacts / "candidate_review_manifest.json")
        for clip_id in (
            "candidate_review_clip_000001",
            "candidate_review_clip_000002",
            "candidate_review_clip_000003",
            "candidate_review_clip_000004",
        ):
            (clips_dir / f"{clip_id}.wav").write_bytes(b"RIFF")
        if include_qc_artifacts:
            shutil.copy(FIXTURES_ROOT / "transcript_qc.json", artifacts / "transcript_qc.json")
            shutil.copy(FIXTURES_ROOT / "speaker_purity.json", artifacts / "speaker_purity.json")

    def test_get_qc_not_ready_when_artifacts_missing(self) -> None:
        run_root = self.repository.media_root / str(self.run.artifact_root)
        (run_root / "artifacts" / "transcript_qc.json").unlink()

        payload = get_dataset_qc(self.repository, self.run.id)

        self.assertFalse(payload.ready)
        self.assertEqual(payload.missing_artifacts, ["artifacts/transcript_qc.json"])
        self.assertEqual(payload.invalid_artifacts, [])
        self.assertEqual(payload.clips, [])

    def test_get_qc_not_ready_when_manifest_missing(self) -> None:
        run_root = self.repository.media_root / str(self.run.artifact_root)
        (run_root / "artifacts" / "candidate_review_manifest.json").unlink()

        payload = get_dataset_qc(self.repository, self.run.id)

        self.assertFalse(payload.ready)
        self.assertIn(CANDIDATE_MANIFEST_REL, payload.missing_artifacts)
        self.assertEqual(payload.clips, [])

    def test_get_qc_aggregates_manifest_and_qc_artifacts(self) -> None:
        payload = get_dataset_qc(self.repository, self.run.id)

        self.assertTrue(payload.ready)
        self.assertEqual(payload.missing_artifacts, [])
        self.assertEqual(payload.invalid_artifacts, [])
        self.assertEqual(payload.defaults.transcript_match_threshold, DEFAULT_TRANSCRIPT_THRESHOLD)
        self.assertEqual(payload.defaults.speaker_check_threshold, DEFAULT_SPEAKER_THRESHOLD)
        self.assertEqual(len(payload.clips), 4)

        by_id = {clip.clip_id: clip for clip in payload.clips}
        clip_001 = by_id["candidate_review_clip_000001"]
        self.assertEqual(clip_001.transcript_match, 87)
        self.assertEqual(clip_001.speaker_check, 74)
        self.assertEqual(len(clip_001.weak_transcript_spans), 1)
        self.assertEqual(clip_001.weak_transcript_spans[0].score, 87.0)
        self.assertEqual(len(clip_001.weak_speaker_spans), 1)
        self.assertEqual(clip_001.weak_speaker_spans[0].score, 74.0)
        self.assertIn("/media/dataset-runs/", clip_001.audio_url)

        missing_both = by_id["candidate_review_clip_000004"]
        self.assertIsNone(missing_both.transcript_match)
        self.assertIsNone(missing_both.speaker_check)
        self.assertIn("missing_transcript_qc", missing_both.qc_reason_codes)
        self.assertIn("missing_speaker_qc", missing_both.qc_reason_codes)

    def test_per_clip_malformed_score_treated_as_missing(self) -> None:
        run_root = self.repository.media_root / str(self.run.artifact_root)
        transcript = json.loads((run_root / "artifacts" / "transcript_qc.json").read_text(encoding="utf-8"))
        transcript["clips"][0]["transcript_match_score"] = 87.5
        (run_root / "artifacts" / "transcript_qc.json").write_text(json.dumps(transcript), encoding="utf-8")

        payload = get_dataset_qc(self.repository, self.run.id)
        clip = next(row for row in payload.clips if row.clip_id == "candidate_review_clip_000001")

        self.assertIsNone(clip.transcript_match)
        self.assertIn("missing_transcript_qc", clip.qc_reason_codes)

    def test_missing_audio_file_is_reported_and_rejected_on_finalize(self) -> None:
        run_root = self.repository.media_root / str(self.run.artifact_root)
        (run_root / "artifacts" / "candidate_review_clips" / "candidate_review_clip_000001.wav").unlink()

        payload = get_dataset_qc(self.repository, self.run.id)
        clip = next(row for row in payload.clips if row.clip_id == "candidate_review_clip_000001")
        self.assertIn("missing_audio_file", clip.qc_reason_codes)

        finalize_dataset_qc(
            self.repository,
            self.run.id,
            DatasetQcFinalizeRequest(
                thresholds=DatasetQcThresholdsRequest(transcript_match_min=0, speaker_check_min=0),
            ),
        )
        dataset_qc = json.loads((run_root / "artifacts" / "dataset_qc.json").read_text(encoding="utf-8"))
        clip_row = next(row for row in dataset_qc["clips"] if row["clip_id"] == "candidate_review_clip_000001")
        self.assertEqual(clip_row["status"], "rejected")
        self.assertIn("missing_audio_file", clip_row["failed_checks"])

    def test_force_keep_cannot_accept_missing_audio_file(self) -> None:
        run_root = self.repository.media_root / str(self.run.artifact_root)
        (run_root / "artifacts" / "candidate_review_clips" / "candidate_review_clip_000001.wav").unlink()

        with self.assertRaisesRegex(ValueError, "Cannot force_keep clip with missing audio"):
            finalize_dataset_qc(
                self.repository,
                self.run.id,
                DatasetQcFinalizeRequest(
                    thresholds=DatasetQcThresholdsRequest(transcript_match_min=0, speaker_check_min=0),
                    manual_overrides=[
                        DatasetQcManualOverrideRequest(
                            clip_id="candidate_review_clip_000001",
                            override="force_keep",
                        )
                    ],
                ),
            )

    def test_invalid_manifest_duration_reports_invalid_manifest(self) -> None:
        run_root = self.repository.media_root / str(self.run.artifact_root)
        manifest = json.loads((run_root / "artifacts" / "candidate_review_manifest.json").read_text(encoding="utf-8"))
        manifest[0]["duration_sec"] = "bad"
        (run_root / "artifacts" / "candidate_review_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

        payload = get_dataset_qc(self.repository, self.run.id)

        self.assertFalse(payload.ready)
        self.assertEqual(payload.invalid_artifacts, [CANDIDATE_MANIFEST_REL])
        self.assertEqual(payload.clips, [])

    def test_duplicate_clip_id_in_manifest_is_invalid(self) -> None:
        run_root = self.repository.media_root / str(self.run.artifact_root)
        manifest = json.loads((run_root / "artifacts" / "candidate_review_manifest.json").read_text(encoding="utf-8"))
        manifest.append(dict(manifest[0]))
        (run_root / "artifacts" / "candidate_review_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

        payload = get_dataset_qc(self.repository, self.run.id)

        self.assertFalse(payload.ready)
        self.assertEqual(payload.invalid_artifacts, [CANDIDATE_MANIFEST_REL])

    def test_corrupt_dataset_qc_still_returns_aggregate(self) -> None:
        run_root = self.repository.media_root / str(self.run.artifact_root)
        (run_root / "artifacts" / "dataset_qc.json").write_text("{not valid json", encoding="utf-8")

        payload = get_dataset_qc(self.repository, self.run.id)

        self.assertTrue(payload.ready)
        self.assertFalse(payload.finalized)
        self.assertEqual(payload.invalid_artifacts, [DATASET_QC_REL])
        self.assertEqual(len(payload.clips), 4)

    def test_finalize_preserves_all_reason_codes(self) -> None:
        finalize_dataset_qc(
            self.repository,
            self.run.id,
            DatasetQcFinalizeRequest(
                thresholds=DatasetQcThresholdsRequest(transcript_match_min=85, speaker_check_min=70),
            ),
        )
        run_root = self.repository.media_root / str(self.run.artifact_root)
        dataset_qc = json.loads((run_root / "artifacts" / "dataset_qc.json").read_text(encoding="utf-8"))
        summary = json.loads((run_root / "artifacts" / "dataset_qc_summary.json").read_text(encoding="utf-8"))

        clip_002 = next(row for row in dataset_qc["clips"] if row["clip_id"] == "candidate_review_clip_000002")
        self.assertIn("low_transcript_match", clip_002["reason_codes"])
        self.assertIn("weak_transcript_span", clip_002["reason_codes"])

        clip_003 = next(row for row in dataset_qc["clips"] if row["clip_id"] == "candidate_review_clip_000003")
        self.assertIn("clip_contains_oov", clip_003["reason_codes"])
        self.assertIn("low_speaker_similarity_window", clip_003["reason_codes"])
        self.assertIn("possible_other_speaker_or_voice_shift", clip_003["reason_codes"])
        self.assertGreater(summary["reason_counts"].get("clip_contains_oov", 0), 0)

    def test_finalize_writes_versioned_artifacts_and_overwrites(self) -> None:
        request = DatasetQcFinalizeRequest(
            thresholds=DatasetQcThresholdsRequest(transcript_match_min=85, speaker_check_min=70),
            manual_overrides=[
                DatasetQcManualOverrideRequest(
                    clip_id="candidate_review_clip_000003",
                    override="force_keep",
                    reason="user_listened_and_accepted",
                )
            ],
        )
        first = finalize_dataset_qc(self.repository, self.run.id, request)
        run_root = self.repository.media_root / str(self.run.artifact_root)
        dataset_qc_path = run_root / "artifacts" / "dataset_qc.json"
        summary_path = run_root / "artifacts" / "dataset_qc_summary.json"
        self.assertTrue(dataset_qc_path.exists())
        self.assertTrue(summary_path.exists())

        dataset_qc = json.loads(dataset_qc_path.read_text(encoding="utf-8"))
        self.assertEqual(dataset_qc["schema_version"], 1)
        self.assertEqual(dataset_qc["thresholds"]["transcript_match_min"], 85)
        created_at = dataset_qc["created_at"]

        clip_003 = next(row for row in dataset_qc["clips"] if row["clip_id"] == "candidate_review_clip_000003")
        self.assertEqual(clip_003["status"], "accepted")
        self.assertEqual(clip_003["manual_override"], "force_keep")

        clip_004 = next(row for row in dataset_qc["clips"] if row["clip_id"] == "candidate_review_clip_000004")
        self.assertEqual(clip_004["status"], "rejected")
        self.assertIn("missing_transcript_qc", clip_004["failed_checks"])

        self.assertGreater(first.summary.accepted_count, 0)

        second_request = DatasetQcFinalizeRequest(
            thresholds=DatasetQcThresholdsRequest(transcript_match_min=90, speaker_check_min=75),
            manual_overrides=[],
        )
        finalize_dataset_qc(self.repository, self.run.id, second_request)
        dataset_qc_second = json.loads(dataset_qc_path.read_text(encoding="utf-8"))
        self.assertEqual(dataset_qc_second["created_at"], created_at)
        self.assertEqual(dataset_qc_second["thresholds"]["transcript_match_min"], 90)
        self.assertNotEqual(dataset_qc_second["updated_at"], created_at)

        refreshed = refresh_dataset_run(self.repository, self.run.id)
        kinds = {artifact.kind for artifact in refreshed.artifacts}
        self.assertIn(RunArtifactKind.DATASET_QC_JSON, kinds)
        self.assertIn(RunArtifactKind.DATASET_QC_SUMMARY_JSON, kinds)

    def test_finalize_rejects_invalid_thresholds_and_overrides(self) -> None:
        with self.assertRaises(ValidationError):
            DatasetQcThresholdsRequest(transcript_match_min=85.5, speaker_check_min=70)

        with self.assertRaisesRegex(ValueError, "Unknown clip_id"):
            finalize_dataset_qc(
                self.repository,
                self.run.id,
                DatasetQcFinalizeRequest(
                    thresholds=DatasetQcThresholdsRequest(transcript_match_min=85, speaker_check_min=70),
                    manual_overrides=[
                        DatasetQcManualOverrideRequest(
                            clip_id="candidate_review_clip_missing",
                            override="force_keep",
                        )
                    ],
                ),
            )

    def test_finalize_not_ready_without_qc_artifacts(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        run_root = self.repository.media_root / str(run.artifact_root)
        self._install_qc_fixtures(run_root, include_qc_artifacts=False)

        with self.assertRaisesRegex(ValueError, "QC is not ready"):
            finalize_dataset_qc(
                self.repository,
                run.id,
                DatasetQcFinalizeRequest(
                    thresholds=DatasetQcThresholdsRequest(transcript_match_min=85, speaker_check_min=70),
                ),
            )

    def test_get_qc_reflects_finalized_state(self) -> None:
        finalize_dataset_qc(
            self.repository,
            self.run.id,
            DatasetQcFinalizeRequest(
                thresholds=DatasetQcThresholdsRequest(transcript_match_min=88, speaker_check_min=72),
                manual_overrides=[
                    DatasetQcManualOverrideRequest(
                        clip_id="candidate_review_clip_000002",
                        override="force_reject",
                    )
                ],
            ),
        )
        payload = get_dataset_qc(self.repository, self.run.id)

        self.assertTrue(payload.finalized)
        self.assertIsNotNone(payload.finalized_thresholds)
        assert payload.finalized_thresholds is not None
        self.assertEqual(payload.finalized_thresholds.transcript_match_min, 88)
        clip = next(row for row in payload.clips if row.clip_id == "candidate_review_clip_000002")
        self.assertEqual(clip.manual_override, "force_reject")

    def test_unsupported_schema_version_reports_invalid_artifact(self) -> None:
        run_root = self.repository.media_root / str(self.run.artifact_root)
        transcript = json.loads((run_root / "artifacts" / "transcript_qc.json").read_text(encoding="utf-8"))
        transcript["schema_version"] = 99
        (run_root / "artifacts" / "transcript_qc.json").write_text(json.dumps(transcript), encoding="utf-8")

        payload = get_dataset_qc(self.repository, self.run.id)

        self.assertFalse(payload.ready)
        self.assertEqual(payload.invalid_artifacts, [TRANSCRIPT_QC_REL])
        self.assertEqual(payload.clips, [])

    def test_duplicate_clip_id_in_qc_artifact_is_invalid(self) -> None:
        run_root = self.repository.media_root / str(self.run.artifact_root)
        transcript = json.loads((run_root / "artifacts" / "transcript_qc.json").read_text(encoding="utf-8"))
        transcript["clips"].append(dict(transcript["clips"][0]))
        (run_root / "artifacts" / "transcript_qc.json").write_text(json.dumps(transcript), encoding="utf-8")

        payload = get_dataset_qc(self.repository, self.run.id)

        self.assertFalse(payload.ready)
        self.assertIn(TRANSCRIPT_QC_REL, payload.invalid_artifacts)


if __name__ == "__main__":
    unittest.main()
