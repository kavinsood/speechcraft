from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
import wave
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np

from fastapi.testclient import TestClient
from sqlmodel import Session

from app.clip_lab_audio import MAX_INSERT_SILENCE_SEC
from app.clip_lab_state import ClipLabStateError, compute_content_hash, compute_manifest_sha256
from app.dataset_runs import create_dataset_run
from app.main import app, patch_dataset_clip_lab_route, read_dataset_clip_lab
from app.models import (
    DatasetClipLabClipView,
    DatasetClipLabPatchRequest,
    DatasetClipLabPipelineFindingView,
    DatasetClipLabView,
    DatasetRunCreateRequest,
    DatasetSlicerRerunRequest,
    ImportBatch,
    SourceRecording,
)
from app.repository import SQLiteRepository

FIXTURES_ROOT = Path(__file__).resolve().parent / "fixtures" / "qc"
SAMPLE_RATE = 16000


def _install_manifest_audio_files(artifacts: Path, manifest: list[dict]) -> list[dict]:
    import hashlib
    import wave

    import numpy as np

    clips_dir = artifacts / "candidate_review_clips"
    clips_dir.mkdir(parents=True, exist_ok=True)
    for index, row in enumerate(manifest):
        clip_id = str(row["id"])
        samples = list(range(100 + index * 10, 160 + index * 10))
        wav_path = clips_dir / f"{clip_id}.wav"
        wav_path.parent.mkdir(parents=True, exist_ok=True)
        with wave.open(str(wav_path), "wb") as handle:
            handle.setnchannels(1)
            handle.setsampwidth(2)
            handle.setframerate(SAMPLE_RATE)
            handle.writeframes(np.asarray(samples, dtype=np.int16).tobytes())
        digest = hashlib.sha256(wav_path.read_bytes()).hexdigest()
        row["audio_sha256"] = digest
        row["audio_hash"] = digest
        row["sample_rate"] = SAMPLE_RATE
        row["duration_samples"] = len(samples)
    return manifest


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
        manifest = json.loads((artifacts / "candidate_review_manifest.json").read_text(encoding="utf-8"))
        manifest = _install_manifest_audio_files(artifacts, manifest)
        (artifacts / "candidate_review_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
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

    def _clip_row(self) -> dict:
        with self.repo():
            response = self.client.get(f"/api/dataset-runs/{self.run.id}/clip-lab")
        self.assertEqual(response.status_code, 200)
        return next(row for row in response.json()["clips"] if row["clip_id"] == "candidate_review_clip_000001")

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
            base_audio_hash=body["source_audio_sha256"],
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
            base_audio_hash=body["source_audio_sha256"],
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

    def test_get_clip_lab_returns_503_while_run_lock_is_held(self) -> None:
        from app.clip_lab_state import clip_lab_run_lock

        with clip_lab_run_lock(self.run_root):
            with self.repo():
                response = self.client.get(f"/api/dataset-runs/{self.run.id}/clip-lab")

        self.assertEqual(response.status_code, 503)
        self.assertIn("busy", response.json()["detail"].lower())

    def test_candidate_review_media_returns_503_while_run_lock_is_held(self) -> None:
        from app.clip_lab_state import clip_lab_run_lock

        with clip_lab_run_lock(self.run_root):
            with self.repo():
                response = self.client.get(
                    f"/media/dataset-runs/{self.run.id}/candidate-review/candidate_review_clip_000001.wav"
                )

        self.assertEqual(response.status_code, 503)
        self.assertIn("busy", response.json()["detail"].lower())

    def test_slicer_results_returns_503_while_run_lock_is_held(self) -> None:
        from app.clip_lab_state import clip_lab_run_lock

        with clip_lab_run_lock(self.run_root):
            with self.repo():
                response = self.client.get(f"/api/dataset-runs/{self.run.id}/slicer-results")

        self.assertEqual(response.status_code, 503)
        self.assertIn("busy", response.json()["detail"].lower())

    def test_clip_lab_and_slicer_results_both_busy_during_promotion_lock(self) -> None:
        from app.clip_lab_state import clip_lab_run_lock

        with clip_lab_run_lock(self.run_root):
            with self.repo():
                clip_lab_response = self.client.get(f"/api/dataset-runs/{self.run.id}/clip-lab")
                slicer_response = self.client.get(f"/api/dataset-runs/{self.run.id}/slicer-results")

        self.assertEqual(clip_lab_response.status_code, 503)
        self.assertEqual(slicer_response.status_code, 503)

    def test_malformed_qc_artifact_reports_qc_error(self) -> None:
        artifacts = self.run_root / "artifacts"
        (artifacts / "transcript_qc.json").write_text(json.dumps(["bad-row"]), encoding="utf-8")
        with self.repo():
            response = self.client.get(f"/api/dataset-runs/{self.run.id}/clip-lab")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["qc_available"])
        self.assertIsNotNone(payload["qc_error"])

    def test_slicer_rerun_returns_409_when_clip_lab_lock_is_held(self) -> None:
        from app.clip_lab_state import clip_lab_run_lock

        artifacts = self.run_root / "artifacts"
        for relative in ("asr_mfa_queue.json", "aligned_words.jsonl", "alignment_qc_by_buffer.json"):
            path = artifacts / relative
            if not path.exists():
                path.write_text("[]" if relative.endswith(".json") else "", encoding="utf-8")

        with clip_lab_run_lock(self.run_root):
            with (
                self.repo(),
                patch("app.dataset_runs.dataset_worker_python", return_value=Path("/bin/true")),
                patch("app.dataset_runs.dataset_worker_root", return_value=Path("/tmp/worker")),
                patch("app.dataset_runs.subprocess.Popen", return_value=Mock(pid=1234)) as popen,
            ):
                response = self.client.post(
                    f"/api/dataset-runs/{self.run.id}/slicer-rerun",
                    json={"config": {}},
                )

        self.assertEqual(response.status_code, 409)
        self.assertIn("busy", response.json()["detail"].lower())
        popen.assert_not_called()


class ClipLabAudioRouteHttpTests(unittest.TestCase):
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
        manifest = json.loads((artifacts / "candidate_review_manifest.json").read_text(encoding="utf-8"))
        manifest = _install_manifest_audio_files(artifacts, manifest)
        (artifacts / "candidate_review_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        self.manifest_sha = compute_manifest_sha256(artifacts / "candidate_review_manifest.json")
        self.clip_id = "candidate_review_clip_000001"
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.repository.close()
        self.temp_dir.cleanup()

    @contextmanager
    def repo(self):
        with patch("app.main.repository", self.repository):
            yield

    def _clip_row(self) -> dict:
        with self.repo():
            response = self.client.get(f"/api/dataset-runs/{self.run.id}/clip-lab")
        self.assertEqual(response.status_code, 200)
        return next(row for row in response.json()["clips"] if row["clip_id"] == self.clip_id)

    def test_audio_delete_operation_renders_cache_and_peaks(self) -> None:
        clip = self._clip_row()
        with self.repo():
            response = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/operations",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": clip["clip_version"],
                    "operation": {"kind": "delete_range", "start_sample": 10, "end_sample": 20},
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["clip_version"], clip["clip_version"] + 1)
        self.assertEqual(body["render_status"], "ready")
        self.assertEqual(body["audio_edit_op_count"], 1)
        self.assertTrue(body["can_undo_audio"])
        self.assertIsNotNone(body["audio_revision_hash"])
        self.assertNotEqual(body["effective_audio_revision_key"], body["source_audio_sha256"])

        revision_key = body["effective_audio_revision_key"]
        render_path = (
            self.run_root
            / "artifacts/clip_lab_renders"
            / self.clip_id
            / f"{revision_key}.wav"
        )
        peaks_path = self.run_root / "artifacts/clip_lab_peaks" / f"{revision_key}.json"
        self.assertTrue(render_path.is_file())
        self.assertTrue(peaks_path.is_file())

        with self.repo():
            audio_response = self.client.get(
                f"/media/dataset-runs/{self.run.id}/clip-lab/{self.clip_id}/audio/{revision_key}.wav"
            )
            peaks_response = self.client.get(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/waveform-peaks/{revision_key}"
            )
        self.assertEqual(audio_response.status_code, 200)
        self.assertEqual(peaks_response.status_code, 200)
        peaks_payload = peaks_response.json()
        self.assertEqual(peaks_payload["revision_key"], revision_key)
        self.assertEqual(peaks_payload["bins"], 960)
        self.assertEqual(len(peaks_payload["peaks"]), 960)

    def test_audio_undo_all_restores_original_revision_key(self) -> None:
        clip = self._clip_row()
        with self.repo():
            op_response = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/operations",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": clip["clip_version"],
                    "operation": {"kind": "delete_range", "start_sample": 5, "end_sample": 15},
                },
            )
        edited = op_response.json()
        with self.repo():
            undo_response = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/undo",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": edited["clip_version"],
                },
            )
        self.assertEqual(undo_response.status_code, 200)
        body = undo_response.json()
        self.assertEqual(body["effective_audio_revision_key"], body["source_audio_sha256"])
        self.assertEqual(body["audio_edit_op_count"], 0)
        self.assertTrue(body["can_redo_audio"])
        self.assertEqual(body["render_status"], "ready")
        state = json.loads((self.run_root / "artifacts/clip_lab_state.json").read_text(encoding="utf-8"))
        audio_edit = state["clips"][self.clip_id]["audio_edit"]
        self.assertEqual(audio_edit["ops"], [])
        self.assertEqual(len(audio_edit["redo_ops"]), 1)

    def test_audio_redo_restores_same_revision_hash(self) -> None:
        clip = self._clip_row()
        with self.repo():
            op_response = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/operations",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": clip["clip_version"],
                    "operation": {"kind": "delete_range", "start_sample": 5, "end_sample": 15},
                },
            )
        edited = op_response.json()
        revision_hash = edited["audio_revision_hash"]
        with self.repo():
            self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/undo",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": edited["clip_version"],
                },
            )
            redo_response = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/redo",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": edited["clip_version"] + 1,
                },
            )
        self.assertEqual(redo_response.status_code, 200)
        self.assertEqual(redo_response.json()["audio_revision_hash"], revision_hash)

    def test_accept_while_audio_pending_returns_422(self) -> None:
        clip = self._clip_row()
        with self.repo():
            op_response = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/operations",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": clip["clip_version"],
                    "operation": {"kind": "delete_range", "start_sample": 5, "end_sample": 15},
                },
            )
        edited = op_response.json()
        state_path = self.run_root / "artifacts/clip_lab_state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["clips"][self.clip_id]["audio_edit"]["render_status"] = "pending"
        state_path.write_text(json.dumps(state), encoding="utf-8")
        with self.repo():
            accept_response = self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": edited["clip_version"],
                    "review_status": "accepted",
                },
            )
        self.assertEqual(accept_response.status_code, 422)

    def test_accept_while_audio_failed_returns_422(self) -> None:
        clip = self._clip_row()
        with self.repo():
            op_response = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/operations",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": clip["clip_version"],
                    "operation": {"kind": "delete_range", "start_sample": 5, "end_sample": 15},
                },
            )
        edited = op_response.json()
        state_path = self.run_root / "artifacts/clip_lab_state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["clips"][self.clip_id]["audio_edit"]["render_status"] = "failed"
        state_path.write_text(json.dumps(state), encoding="utf-8")
        with self.repo():
            accept_response = self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": edited["clip_version"],
                    "review_status": "accepted",
                },
            )
        self.assertEqual(accept_response.status_code, 422)
        self.assertIn("failed", accept_response.json()["detail"].lower())

    def test_audio_operation_validation_rejects_malformed_requests(self) -> None:
        clip = self._clip_row()
        duration_samples = 60
        cases: list[tuple[str, dict, int]] = [
            ("unknown kind", {"kind": "normalize", "start_sample": 0, "end_sample": 1}, 400),
            (
                "unknown field",
                {"kind": "delete_range", "start_sample": 0, "end_sample": 5, "surprise": True},
                400,
            ),
            ("string coordinate", {"kind": "delete_range", "start_sample": "banana", "end_sample": 4}, 400),
            ("bool coordinate", {"kind": "delete_range", "start_sample": True, "end_sample": 4}, 400),
            ("out of bounds", {"kind": "delete_range", "start_sample": 0, "end_sample": 999}, 400),
            ("full clip delete", {"kind": "delete_range", "start_sample": 0, "end_sample": duration_samples}, 400),
            (
                "excessive silence",
                {
                    "kind": "insert_silence",
                    "at_sample": 0,
                    "duration_samples": int(16000 * MAX_INSERT_SILENCE_SEC) + 1,
                },
                400,
            ),
        ]
        for label, operation, expected_status in cases:
            with self.subTest(label=label):
                with self.repo():
                    response = self.client.post(
                        f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/operations",
                        json={
                            "expected_manifest_sha256": self.manifest_sha,
                            "expected_clip_version": clip["clip_version"],
                            "operation": operation,
                        },
                    )
                self.assertEqual(response.status_code, expected_status, response.text)

    def test_wrong_revision_key_audio_returns_404(self) -> None:
        with self.repo():
            response = self.client.get(
                f"/media/dataset-runs/{self.run.id}/clip-lab/{self.clip_id}/audio/not-a-real-revision.wav"
            )
        self.assertEqual(response.status_code, 404)

    def test_render_failure_leaves_state_recoverable_with_undo(self) -> None:
        clip = self._clip_row()
        with self.repo(), patch(
            "app.clip_lab_audio_ops._perform_sync_render",
            side_effect=RuntimeError("render failed"),
        ):
            response = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/operations",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": clip["clip_version"],
                    "operation": {"kind": "delete_range", "start_sample": 5, "end_sample": 15},
                },
            )
        self.assertEqual(response.status_code, 500)
        state = json.loads((self.run_root / "artifacts/clip_lab_state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["clips"][self.clip_id]["audio_edit"]["render_status"], "failed")
        with self.repo():
            get_response = self.client.get(f"/api/dataset-runs/{self.run.id}/clip-lab")
        self.assertEqual(get_response.status_code, 200)
        failed_clip = next(row for row in get_response.json()["clips"] if row["clip_id"] == self.clip_id)
        self.assertEqual(failed_clip["render_status"], "failed")
        self.assertEqual(failed_clip["effective_audio_kind"], "candidate_original")
        self.assertEqual(failed_clip["effective_audio_revision_key"], failed_clip["source_audio_sha256"])
        self.assertGreater(failed_clip["audio_edit_op_count"], 0)
        self.assertTrue(failed_clip["can_undo_audio"])
        self.assertIn(failed_clip["source_audio_sha256"], failed_clip["audio_url"])
        with self.repo():
            audio_response = self.client.get(failed_clip["audio_url"])
        self.assertEqual(audio_response.status_code, 200)
        with self.repo():
            undo_response = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/undo",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": clip["clip_version"] + 1,
                },
            )
        self.assertEqual(undo_response.status_code, 200)
        self.assertEqual(undo_response.json()["audio_edit_op_count"], 0)

    def test_finalize_failure_returns_409(self) -> None:
        clip = self._clip_row()
        with self.repo(), patch("app.clip_lab_audio_ops._finalize_render", return_value=False):
            response = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/operations",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": clip["clip_version"],
                    "operation": {"kind": "delete_range", "start_sample": 5, "end_sample": 15},
                },
            )
        self.assertEqual(response.status_code, 409)

    def test_pending_edit_reports_source_duration(self) -> None:
        clip = self._clip_row()
        with self.repo(), patch(
            "app.clip_lab_audio_ops._perform_sync_render",
            side_effect=RuntimeError("render failed"),
        ):
            response = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/operations",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": clip["clip_version"],
                    "operation": {"kind": "delete_range", "start_sample": 5, "end_sample": 15},
                },
            )
        self.assertEqual(response.status_code, 500)
        with self.repo():
            get_response = self.client.get(f"/api/dataset-runs/{self.run.id}/clip-lab")
        failed_clip = next(row for row in get_response.json()["clips"] if row["clip_id"] == self.clip_id)
        expected_source_duration = round(60 / SAMPLE_RATE, 6)
        self.assertEqual(failed_clip["render_status"], "failed")
        self.assertEqual(failed_clip["current_duration_sec"], expected_source_duration)

    def test_ready_edit_reports_edited_duration(self) -> None:
        clip = self._clip_row()
        with self.repo():
            response = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/operations",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": clip["clip_version"],
                    "operation": {"kind": "delete_range", "start_sample": 5, "end_sample": 15},
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["render_status"], "ready")
        self.assertEqual(body["effective_audio_kind"], "rendered_revision")
        self.assertLess(body["current_duration_sec"], round(60 / SAMPLE_RATE, 6))

    def test_accept_edited_clip_with_missing_render_cache_returns_422(self) -> None:
        clip = self._clip_row()
        with self.repo():
            edited = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/operations",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": clip["clip_version"],
                    "operation": {"kind": "delete_range", "start_sample": 5, "end_sample": 15},
                },
            ).json()
        revision_key = edited["audio_revision_hash"]
        cache_path = (
            self.run_root / "artifacts/clip_lab_renders" / self.clip_id / f"{revision_key}.wav"
        )
        cache_path.unlink()
        with self.repo():
            accept_response = self.client.patch(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/clip-lab",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": edited["clip_version"],
                    "review_status": "accepted",
                },
            )
        self.assertEqual(accept_response.status_code, 422)
        self.assertIn("cache", accept_response.json()["detail"].lower())

    def test_peak_get_does_not_render_under_run_lock(self) -> None:
        from app.clip_lab_state import clip_lab_run_lock

        clip = self._clip_row()
        with self.repo():
            edited = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/operations",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": clip["clip_version"],
                    "operation": {"kind": "delete_range", "start_sample": 5, "end_sample": 15},
                },
            ).json()
        revision_key = edited["effective_audio_revision_key"]
        peaks_path = self.run_root / "artifacts/clip_lab_peaks" / f"{revision_key}.json"
        peaks_path.unlink(missing_ok=True)
        with self.repo(), patch(
            "app.clip_lab_audio_ops.render_or_reuse_audio_revision_from_bytes"
        ) as render_mock:
            response = self.client.get(edited["waveform_peaks_url"])
        self.assertEqual(response.status_code, 409)
        render_mock.assert_not_called()

    def test_manifest_regeneration_during_render_returns_409(self) -> None:
        import app.clip_lab_audio_ops as ops_module

        clip = self._clip_row()
        real_render = ops_module._perform_sync_render

        def render_after_manifest_swap(job: ops_module._RenderJob) -> str:
            manifest_path = job.run_root / "artifacts/candidate_review_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            wav_path = job.run_root / "artifacts/candidate_review_clips" / f"{self.clip_id}.wav"
            with wave.open(str(wav_path), "wb") as handle:
                handle.setnchannels(1)
                handle.setsampwidth(2)
                handle.setframerate(SAMPLE_RATE)
                handle.writeframes(np.zeros(80, dtype=np.int16).tobytes())
            new_sha = hashlib.sha256(wav_path.read_bytes()).hexdigest()
            manifest[0]["audio_sha256"] = new_sha
            manifest[0]["audio_hash"] = new_sha
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            return real_render(job)

        with self.repo(), patch.object(ops_module, "_perform_sync_render", side_effect=render_after_manifest_swap):
            response = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/operations",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": clip["clip_version"],
                    "operation": {"kind": "delete_range", "start_sample": 5, "end_sample": 15},
                },
            )
        self.assertEqual(response.status_code, 409)

    def test_clip_removed_after_render_returns_404(self) -> None:
        import app.clip_lab_audio_ops as ops_module

        clip = self._clip_row()
        real_finalize = ops_module._finalize_render

        def finalize_then_remove_clip(*args, **kwargs):
            result = real_finalize(*args, **kwargs)
            manifest_path = self.run_root / "artifacts/candidate_review_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest_path.write_text(
                json.dumps([row for row in manifest if row.get("id") != self.clip_id]),
                encoding="utf-8",
            )
            return result

        with self.repo(), patch.object(ops_module, "_finalize_render", side_effect=finalize_then_remove_clip):
            response = self.client.post(
                f"/api/dataset-runs/{self.run.id}/clips/{self.clip_id}/audio/operations",
                json={
                    "expected_manifest_sha256": self.manifest_sha,
                    "expected_clip_version": clip["clip_version"],
                    "operation": {"kind": "delete_range", "start_sample": 5, "end_sample": 15},
                },
            )
        self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
