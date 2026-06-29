from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlmodel import Session

from app.clip_lab_state import ClipLabStateError, compute_content_hash, compute_manifest_sha256
from app.dataset_runs import create_dataset_run
from app.main import app, patch_dataset_clip_lab_route, read_dataset_clip_lab
from app.models import (
    DatasetClipLabClipView,
    DatasetClipLabPatchRequest,
    DatasetClipLabPipelineFindingView,
    DatasetClipLabView,
    DatasetRunCreateRequest,
    ImportBatch,
    SourceRecording,
)
from app.repository import SQLiteRepository

FIXTURES_ROOT = Path(__file__).resolve().parent / "fixtures" / "qc"


def clip_lab_view(run_id: str = "dataset-run-1") -> DatasetClipLabView:
    return DatasetClipLabView(
        run_id=run_id,
        candidate_manifest_sha256="abc123",
        qc_available=True,
        clips=[
            DatasetClipLabClipView(
                clip_id="candidate_review_clip_000001",
                clip_version=0,
                review_status="unresolved",
                transcript="Hello world",
                original_transcript="Hello world",
                content_hash="hash-1",
                pipeline_findings=[
                    DatasetClipLabPipelineFindingView(code="clip_contains_oov", label="clip contains OOV"),
                ],
            )
        ],
    )


class ClipLabRouteUnitTests(unittest.TestCase):
    def test_read_route_serializes(self) -> None:
        payload = clip_lab_view()
        with patch("app.main.get_dataset_clip_lab", return_value=payload) as get_clip_lab:
            response = read_dataset_clip_lab(payload.run_id)

        self.assertEqual(response.run_id, payload.run_id)
        self.assertEqual(response.clips[0].clip_id, "candidate_review_clip_000001")
        get_clip_lab.assert_called_once()

    def test_patch_route_serializes(self) -> None:
        updated = DatasetClipLabClipView(
            clip_id="candidate_review_clip_000001",
            clip_version=1,
            review_status="accepted",
            transcript="Hello world",
            original_transcript="Hello world",
            content_hash="hash-1",
            accepted_content_hash="hash-1",
            accepted_at="2026-06-26T00:00:00Z",
        )
        with patch("app.main.patch_dataset_clip_lab_clip", return_value=updated) as patch_clip:
            response = patch_dataset_clip_lab_route(
                "dataset-run-1",
                "candidate_review_clip_000001",
                DatasetClipLabPatchRequest(
                    expected_manifest_sha256="abc123",
                    expected_clip_version=0,
                    review_status="accepted",
                ),
            )

        self.assertEqual(response.review_status, "accepted")
        self.assertEqual(response.clip_version, 1)
        patch_clip.assert_called_once()


class ClipLabRouteHttpTests(unittest.TestCase):
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
        artifacts = self.run_root / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURES_ROOT / "candidate_review_manifest.json", artifacts / "candidate_review_manifest.json")
        shutil.copy(FIXTURES_ROOT / "transcript_qc.json", artifacts / "transcript_qc.json")
        shutil.copy(FIXTURES_ROOT / "speaker_purity.json", artifacts / "speaker_purity.json")
        self.manifest_sha = compute_manifest_sha256(artifacts / "candidate_review_manifest.json")
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.repository.close()
        self.temp_dir.cleanup()

    @contextmanager
    def repo(self):
        with patch("app.main.repository", self.repository):
            yield

    def test_get_clip_lab_returns_manifest_clips(self) -> None:
        with self.repo():
            response = self.client.get(f"/api/dataset-runs/{self.run.id}/clip-lab")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["run_id"], self.run.id)
        self.assertFalse(payload["stale_state"])
        self.assertFalse(payload["invalid_state"])
        self.assertTrue(payload["qc_available"])
        self.assertEqual(len(payload["clips"]), 4)

    def test_patch_transcript_override_then_get(self) -> None:
        with self.repo():
            patch_response = self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/candidate_review_clip_000001/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": 0,
                    "transcript_override": "Corrected transcript.",
                },
            )
            self.assertEqual(patch_response.status_code, 200)
            body = patch_response.json()
            self.assertEqual(body["transcript"], "Corrected transcript.")
            self.assertEqual(body["original_transcript"], "I don't think that's what happened.")

            get_response = self.client.get(f"/api/dataset-runs/{self.run.id}/clip-lab")
            clip = next(
                row for row in get_response.json()["clips"] if row["clip_id"] == "candidate_review_clip_000001"
            )
            self.assertEqual(clip["transcript"], "Corrected transcript.")
            self.assertEqual(clip["original_transcript"], "I don't think that's what happened.")

    def test_patch_transcript_override_null_clears_override(self) -> None:
        with self.repo():
            self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/candidate_review_clip_000001/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": 0,
                    "transcript_override": "Temporary override.",
                },
            )
            response = self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/candidate_review_clip_000001/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": 1,
                    "transcript_override": None,
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json()["transcript_override"])
        self.assertEqual(response.json()["transcript"], "I don't think that's what happened.")

    def test_patch_accept_fingerprints_current_content(self) -> None:
        with self.repo():
            response = self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/candidate_review_clip_000001/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": 0,
                    "review_status": "accepted",
                },
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        expected_hash = compute_content_hash(
            manifest_transcript="I don't think that's what happened.",
            transcript_override=None,
            audio_revision_hash=None,
        )
        self.assertEqual(body["accepted_content_hash"], expected_hash)

    def test_patch_accept_with_transcript_override_same_request(self) -> None:
        with self.repo():
            response = self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/candidate_review_clip_000001/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": 0,
                    "transcript_override": "Final transcript.",
                    "review_status": "accepted",
                },
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["review_status"], "accepted")
        expected_hash = compute_content_hash(
            manifest_transcript="I don't think that's what happened.",
            transcript_override="Final transcript.",
            audio_revision_hash=None,
        )
        self.assertEqual(body["accepted_content_hash"], expected_hash)

    def test_patch_accepted_clip_transcript_change_unstages(self) -> None:
        with self.repo():
            self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/candidate_review_clip_000001/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": 0,
                    "review_status": "accepted",
                },
            )
            response = self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/candidate_review_clip_000001/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": 1,
                    "transcript_override": "Changed after accept.",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["review_status"], "unresolved")
        self.assertIsNone(response.json()["accepted_content_hash"])

    def test_patch_empty_mutable_fields_returns_400(self) -> None:
        with self.repo():
            response = self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/candidate_review_clip_000001/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": 0,
                },
            )

        self.assertEqual(response.status_code, 400)

    def test_patch_reserved_reviewer_tag_returns_400(self) -> None:
        with self.repo():
            response = self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/candidate_review_clip_000001/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": 0,
                    "reviewer_tags": ["Accepted"],
                },
            )

        self.assertEqual(response.status_code, 400)

    def test_get_invalid_state_does_not_overlay(self) -> None:
        from app.clip_lab_state import save_clip_lab_state

        save_clip_lab_state(
            self.run_root,
            {
                "schema_version": 1,
                "stage": "clip_lab_state",
                "candidate_manifest_sha256": self.manifest_sha,
                "updated_at": "2026-06-26T00:00:00Z",
                "clips": {
                    "candidate_review_clip_000001": {
                        "clip_version": 1,
                        "review_status": "unresolved",
                        "reviewer_tags": ["Accepted"],
                    }
                },
            },
        )
        with self.repo():
            response = self.client.get(f"/api/dataset-runs/{self.run.id}/clip-lab")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["invalid_state"])
        clip = next(row for row in payload["clips"] if row["clip_id"] == "candidate_review_clip_000001")
        self.assertEqual(clip["reviewer_tags"], [])

    def test_patch_clip_lab_persists_reviewer_tags(self) -> None:
        with self.repo():
            patch_response = self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/candidate_review_clip_000001/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": 0,
                    "reviewer_tags": ["good energy"],
                },
            )
            self.assertEqual(patch_response.status_code, 200)

            get_response = self.client.get(f"/api/dataset-runs/{self.run.id}/clip-lab")
            clip = next(
                row for row in get_response.json()["clips"] if row["clip_id"] == "candidate_review_clip_000001"
            )
            self.assertEqual(clip["reviewer_tags"], ["good energy"])

    def test_patch_clip_lab_stale_version_returns_409(self) -> None:
        with self.repo():
            first = self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/candidate_review_clip_000001/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": 0,
                    "reviewer_tags": ["good energy"],
                },
            )
            self.assertEqual(first.status_code, 200)

            second = self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/candidate_review_clip_000001/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": 0,
                    "reviewer_tags": ["mouth noise"],
                },
            )

        self.assertEqual(second.status_code, 409)

    def test_patch_unknown_clip_returns_404(self) -> None:
        with self.repo():
            response = self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/missing_clip/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": 0,
                    "reviewer_tags": ["good energy"],
                },
            )

        self.assertEqual(response.status_code, 404)

    def test_get_missing_run_returns_404(self) -> None:
        with self.repo():
            response = self.client.get("/api/dataset-runs/missing-run/clip-lab")

        self.assertEqual(response.status_code, 404)

    def test_lock_timeout_returns_503(self) -> None:
        with self.repo(), patch(
            "app.clip_lab_state.clip_lab_run_lock",
            side_effect=ClipLabStateError("Clip Lab state is busy; retry shortly."),
        ):
            response = self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/candidate_review_clip_000001/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": 0,
                    "reviewer_tags": ["good energy"],
                },
            )

        self.assertEqual(response.status_code, 503)
        self.assertIn("busy", response.json()["detail"].lower())

    def test_malformed_qc_artifact_reports_qc_error(self) -> None:
        artifacts = self.run_root / "artifacts"
        (artifacts / "transcript_qc.json").write_text(json.dumps(["bad-row"]), encoding="utf-8")
        with self.repo():
            response = self.client.get(f"/api/dataset-runs/{self.run.id}/clip-lab")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["qc_available"])
        self.assertIsNotNone(payload["qc_error"])


if __name__ == "__main__":
    unittest.main()
