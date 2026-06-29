from __future__ import annotations

import json
import shutil
import tempfile
import threading
import unittest
from pathlib import Path

from app.clip_lab_state import (
  CANDIDATE_MANIFEST_REL,
  CLIP_LAB_STATE_REL,
  ClipLabValidationError,
  ClipNotFoundError,
  StaleClipError,
  StaleManifestError,
  build_clip_lab_view,
  compute_audio_revision_hash,
  compute_content_hash,
  load_candidate_manifest,
  load_clip_lab_state,
  patch_clip_lab_clip,
  save_clip_lab_state,
  validate_reviewer_tags,
)

FIXTURES_ROOT = Path(__file__).resolve().parent / "fixtures" / "qc"


class ClipLabStateTests(unittest.TestCase):
  def setUp(self) -> None:
    self.temp_dir = tempfile.TemporaryDirectory()
    self.run_root = Path(self.temp_dir.name)
    artifacts = self.run_root / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    shutil.copy(FIXTURES_ROOT / "candidate_review_manifest.json", artifacts / "candidate_review_manifest.json")
    shutil.copy(FIXTURES_ROOT / "transcript_qc.json", artifacts / "transcript_qc.json")
    shutil.copy(FIXTURES_ROOT / "speaker_purity.json", artifacts / "speaker_purity.json")

  def tearDown(self) -> None:
    self.temp_dir.cleanup()

  def _manifest_sha(self) -> str:
    from app.clip_lab_state import compute_manifest_sha256

    return compute_manifest_sha256(self.run_root / CANDIDATE_MANIFEST_REL)

  def test_get_merges_manifest_and_defaults(self) -> None:
    view = build_clip_lab_view(self.run_root, run_id="run-1")

    self.assertFalse(view["stale_state"])
    self.assertFalse(view["invalid_state"])
    self.assertEqual(len(view["clips"]), 4)
    clip = next(row for row in view["clips"] if row["clip_id"] == "candidate_review_clip_000001")
    self.assertEqual(clip["review_status"], "unresolved")
    self.assertEqual(clip["original_transcript"], "I don't think that's what happened.")
    self.assertEqual(clip["transcript"], "I don't think that's what happened.")
    self.assertEqual(clip["reviewer_tags"], [])
    self.assertEqual(clip["clip_version"], 0)
    self.assertIsNone(clip["transcript_override"])

  def test_get_includes_pipeline_findings_from_manifest(self) -> None:
    view = build_clip_lab_view(self.run_root, run_id="run-1")
    clip = next(row for row in view["clips"] if row["clip_id"] == "candidate_review_clip_000003")

    self.assertEqual(
      clip["pipeline_findings"],
      [{"code": "clip_contains_oov", "label": "clip contains OOV"}],
    )

  def test_get_stale_manifest_does_not_overlay_saved_state(self) -> None:
    manifest_sha = self._manifest_sha()
    save_clip_lab_state(
      self.run_root,
      {
        "schema_version": 1,
        "stage": "clip_lab_state",
        "candidate_manifest_sha256": "stale-manifest-sha",
        "updated_at": "2026-06-26T00:00:00Z",
        "clips": {
          "candidate_review_clip_000001": {
            "clip_version": 2,
            "review_status": "accepted",
            "accepted_content_hash": "a" * 64,
            "accepted_at": "2026-06-26T00:00:00Z",
            "transcript_override": "Wrong overlay text.",
            "reviewer_tags": ["good energy"],
            "updated_at": "2026-06-26T00:00:00Z",
          }
        },
      },
    )

    view = build_clip_lab_view(self.run_root, run_id="run-1")

    self.assertTrue(view["stale_state"])
    self.assertEqual(view["stale_reason"], "candidate_manifest_changed")
    self.assertEqual(view["saved_state_clip_count"], 1)
    self.assertNotEqual(view["candidate_manifest_sha256"], "stale-manifest-sha")
    clip = next(row for row in view["clips"] if row["clip_id"] == "candidate_review_clip_000001")
    self.assertEqual(clip["review_status"], "unresolved")
    self.assertEqual(clip["reviewer_tags"], [])
    self.assertEqual(clip["transcript"], "I don't think that's what happened.")

  def test_patch_reviewer_tags_increments_clip_version(self) -> None:
    manifest_sha = self._manifest_sha()
    updated = patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=0,
      reviewer_tags=["good energy"],
    )

    self.assertEqual(updated["clip_version"], 1)
    self.assertEqual(updated["reviewer_tags"], ["good energy"])

    state = load_clip_lab_state(self.run_root)
    assert state is not None
    entry = state["clips"]["candidate_review_clip_000001"]
    self.assertEqual(entry["clip_version"], 1)

  def test_patch_transcript_override_on_accepted_clip_unstages(self) -> None:
    manifest_sha = self._manifest_sha()
    patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=0,
      review_status="accepted",
    )

    updated = patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=1,
      transcript_override="Corrected transcript.",
    )

    self.assertEqual(updated["review_status"], "unresolved")
    self.assertEqual(updated["transcript"], "Corrected transcript.")
    self.assertIsNone(updated["accepted_content_hash"])
    self.assertFalse(updated["acceptance_stale"])

  def test_patch_reviewer_tags_only_keeps_accepted_status(self) -> None:
    manifest_sha = self._manifest_sha()
    patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=0,
      review_status="accepted",
    )

    updated = patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=1,
      reviewer_tags=["mouth noise"],
    )

    self.assertEqual(updated["review_status"], "accepted")
    self.assertEqual(updated["reviewer_tags"], ["mouth noise"])
    self.assertIsNotNone(updated["accepted_content_hash"])

  def test_patch_accept_sets_accepted_content_hash(self) -> None:
    manifest_sha = self._manifest_sha()
    updated = patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=0,
      review_status="accepted",
    )

    expected_hash = compute_content_hash(
      manifest_transcript="I don't think that's what happened.",
      transcript_override=None,
      audio_revision_hash=None,
    )
    self.assertEqual(updated["accepted_content_hash"], expected_hash)
    self.assertIsNotNone(updated["accepted_at"])

  def test_validate_reviewer_tags_rejects_reserved_names(self) -> None:
    with self.assertRaises(ClipLabValidationError):
      validate_reviewer_tags(["Accepted"])

    with self.assertRaises(ClipLabValidationError):
      patch_clip_lab_clip(
        self.run_root,
        "candidate_review_clip_000001",
        expected_manifest_sha256=self._manifest_sha(),
        expected_clip_version=0,
        reviewer_tags=["rejected"],
      )

  def test_patch_stale_clip_version_raises(self) -> None:
    manifest_sha = self._manifest_sha()
    patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=0,
      reviewer_tags=["good energy"],
    )

    with self.assertRaises(StaleClipError):
      patch_clip_lab_clip(
        self.run_root,
        "candidate_review_clip_000001",
        expected_manifest_sha256=manifest_sha,
        expected_clip_version=0,
        reviewer_tags=["mouth noise"],
      )

  def test_patch_stale_manifest_raises(self) -> None:
    with self.assertRaises(StaleManifestError):
      patch_clip_lab_clip(
        self.run_root,
        "candidate_review_clip_000001",
        expected_manifest_sha256="not-the-current-manifest",
        expected_clip_version=0,
        reviewer_tags=["good energy"],
      )

  def test_patch_unknown_clip_raises(self) -> None:
    with self.assertRaises(ClipNotFoundError):
      patch_clip_lab_clip(
        self.run_root,
        "missing_clip",
        expected_manifest_sha256=self._manifest_sha(),
        expected_clip_version=0,
        reviewer_tags=["good energy"],
      )

  def test_accept_then_transcript_change_shows_acceptance_stale_on_get(self) -> None:
    manifest_sha = self._manifest_sha()
    accepted = patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=0,
      review_status="accepted",
    )
    accepted_hash = accepted["accepted_content_hash"]

    state = load_clip_lab_state(self.run_root)
    assert state is not None
    entry = state["clips"]["candidate_review_clip_000001"]
    entry["transcript_override"] = "Changed without unstaging in storage."
    entry["review_status"] = "accepted"
    entry["accepted_content_hash"] = accepted_hash
    save_clip_lab_state(self.run_root, state)

    view = build_clip_lab_view(self.run_root, run_id="run-1")
    clip = next(row for row in view["clips"] if row["clip_id"] == "candidate_review_clip_000001")
    self.assertEqual(clip["review_status"], "unresolved")
    self.assertTrue(clip["acceptance_stale"])

  def test_stale_accepted_tag_only_patch_repairs_persisted_state(self) -> None:
    manifest_sha = self._manifest_sha()
    accepted = patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=0,
      review_status="accepted",
    )
    accepted_hash = accepted["accepted_content_hash"]

    state = load_clip_lab_state(self.run_root)
    assert state is not None
    entry = state["clips"]["candidate_review_clip_000001"]
    entry["transcript_override"] = "Changed without unstaging in storage."
    entry["review_status"] = "accepted"
    entry["accepted_content_hash"] = accepted_hash
    save_clip_lab_state(self.run_root, state)

    updated = patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=1,
      reviewer_tags=["good energy"],
    )
    self.assertEqual(updated["review_status"], "unresolved")
    self.assertIsNone(updated["accepted_content_hash"])

    state = load_clip_lab_state(self.run_root)
    assert state is not None
    entry = state["clips"]["candidate_review_clip_000001"]
    self.assertEqual(entry["review_status"], "unresolved")
    self.assertIsNone(entry["accepted_content_hash"])
    self.assertEqual(entry["reviewer_tags"], ["good energy"])

  def test_stale_accepted_status_patch_reaccepts_final_content(self) -> None:
    manifest_sha = self._manifest_sha()
    patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=0,
      review_status="accepted",
    )

    state = load_clip_lab_state(self.run_root)
    assert state is not None
    entry = state["clips"]["candidate_review_clip_000001"]
    entry["transcript_override"] = "Changed without unstaging in storage."
    save_clip_lab_state(self.run_root, state)

    updated = patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=1,
      review_status="accepted",
    )
    self.assertEqual(updated["review_status"], "accepted")
    self.assertIsNotNone(updated["accepted_content_hash"])
    self.assertEqual(updated["transcript"], "Changed without unstaging in storage.")

  def test_manifest_audio_sha256_participates_in_content_hash(self) -> None:
    manifest_path = self.run_root / CANDIDATE_MANIFEST_REL
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest[0]["audio_sha256"] = "a" * 64
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    manifest_sha = self._manifest_sha()

    updated = patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=0,
      review_status="accepted",
    )

    expected_hash = compute_content_hash(
      manifest_transcript="I don't think that's what happened.",
      transcript_override=None,
      audio_revision_hash=None,
      base_audio_hash="a" * 64,
    )
    self.assertEqual(updated["accepted_content_hash"], expected_hash)

  def test_loaded_state_rejects_reserved_reviewer_tags(self) -> None:
    save_clip_lab_state(
      self.run_root,
      {
        "schema_version": 1,
        "stage": "clip_lab_state",
        "candidate_manifest_sha256": self._manifest_sha(),
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

    view = build_clip_lab_view(self.run_root, run_id="run-1")
    self.assertTrue(view["invalid_state"])
    clip = next(row for row in view["clips"] if row["clip_id"] == "candidate_review_clip_000001")
    self.assertEqual(clip["reviewer_tags"], [])

  def test_malformed_qc_artifact_surfaces_qc_error(self) -> None:
    artifacts = self.run_root / "artifacts"
    (artifacts / "transcript_qc.json").write_text(json.dumps(["not-an-object"]), encoding="utf-8")

    view = build_clip_lab_view(self.run_root, run_id="run-1")
    self.assertFalse(view["qc_available"])
    self.assertIsNotNone(view["qc_error"])
    self.assertIn("transcript_qc.json", view["qc_error"])

  def test_concurrent_patches_do_not_lose_updates(self) -> None:
    manifest_sha = self._manifest_sha()
    errors: list[BaseException] = []
    barrier = threading.Barrier(2)

    def worker(tag_name: str) -> None:
      try:
        barrier.wait(timeout=5)
        patch_clip_lab_clip(
          self.run_root,
          "candidate_review_clip_000001",
          expected_manifest_sha256=manifest_sha,
          expected_clip_version=0,
          reviewer_tags=[tag_name],
        )
      except BaseException as exc:  # pragma: no cover - exercised via assertion
        errors.append(exc)

    first = threading.Thread(target=worker, args=("tag-a",))
    second = threading.Thread(target=worker, args=("tag-b",))
    first.start()
    second.start()
    first.join()
    second.join()

    self.assertEqual(len(errors), 1)
    self.assertIsInstance(errors[0], StaleClipError)

    state = load_clip_lab_state(self.run_root)
    assert state is not None
    entry = state["clips"]["candidate_review_clip_000001"]
    self.assertEqual(entry["clip_version"], 1)
    self.assertEqual(len(entry["reviewer_tags"]), 1)

  def test_patch_requires_at_least_one_field(self) -> None:
    with self.assertRaises(ClipLabValidationError):
      patch_clip_lab_clip(
        self.run_root,
        "candidate_review_clip_000001",
        expected_manifest_sha256=self._manifest_sha(),
        expected_clip_version=0,
      )

  def test_clear_transcript_override(self) -> None:
    manifest_sha = self._manifest_sha()
    patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=0,
      transcript_override="Temporary override.",
    )
    updated = patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=1,
      transcript_override=None,
    )

    self.assertIsNone(updated["transcript_override"])
    self.assertEqual(updated["transcript"], "I don't think that's what happened.")

  def test_saved_state_artifact_shape(self) -> None:
    manifest_sha = self._manifest_sha()
    patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=0,
      review_status="accepted",
      reviewer_tags=["good energy"],
    )

    state_path = self.run_root / CLIP_LAB_STATE_REL
    self.assertTrue(state_path.exists())
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    self.assertEqual(payload["schema_version"], 1)
    self.assertEqual(payload["stage"], "clip_lab_state")
    self.assertEqual(payload["candidate_manifest_sha256"], manifest_sha)
    entry = payload["clips"]["candidate_review_clip_000001"]
    self.assertNotIn("original_transcript", entry)
    self.assertNotIn("pipeline_findings", entry)

  def test_patch_accept_with_transcript_override_same_patch_stays_accepted(self) -> None:
    manifest_sha = self._manifest_sha()
    updated = patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=0,
      transcript_override="Corrected final transcript.",
      review_status="accepted",
    )

    expected_hash = compute_content_hash(
      manifest_transcript="I don't think that's what happened.",
      transcript_override="Corrected final transcript.",
      audio_revision_hash=None,
    )
    self.assertEqual(updated["review_status"], "accepted")
    self.assertEqual(updated["transcript"], "Corrected final transcript.")
    self.assertEqual(updated["accepted_content_hash"], expected_hash)
    self.assertFalse(updated["acceptance_stale"])

  def test_patch_accept_with_audio_edl_same_patch_stays_accepted(self) -> None:
    manifest_sha = self._manifest_sha()
    edl = {"ops": [{"kind": "trim", "start_sec": 0.1, "end_sec": 4.5}]}
    audio_hash = compute_audio_revision_hash(edl)
    updated = patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=0,
      audio_edl_recipe=edl,
      review_status="accepted",
    )

    expected_hash = compute_content_hash(
      manifest_transcript="I don't think that's what happened.",
      transcript_override=None,
      audio_revision_hash=audio_hash,
    )
    self.assertEqual(updated["review_status"], "accepted")
    self.assertEqual(updated["accepted_content_hash"], expected_hash)
    state = load_clip_lab_state(self.run_root)
    assert state is not None
    entry = state["clips"]["candidate_review_clip_000001"]
    self.assertEqual(entry["audio_revision_hash"], audio_hash)

  def test_patch_audio_edl_only_on_accepted_clip_unstages(self) -> None:
    manifest_sha = self._manifest_sha()
    patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=0,
      review_status="accepted",
    )

    edl = {"ops": [{"kind": "trim", "start_sec": 0.2, "end_sec": 3.0}]}
    updated = patch_clip_lab_clip(
      self.run_root,
      "candidate_review_clip_000001",
      expected_manifest_sha256=manifest_sha,
      expected_clip_version=1,
      audio_edl_recipe=edl,
    )

    self.assertEqual(updated["review_status"], "unresolved")
    self.assertIsNone(updated["accepted_content_hash"])
    state = load_clip_lab_state(self.run_root)
    assert state is not None
    entry = state["clips"]["candidate_review_clip_000001"]
    self.assertEqual(entry["audio_revision_hash"], compute_audio_revision_hash(edl))

  def test_load_manifest_rejects_non_dict_rows(self) -> None:
    manifest_path = self.run_root / CANDIDATE_MANIFEST_REL
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload.insert(1, "not-an-object")
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with self.assertRaises(ClipLabValidationError):
      load_candidate_manifest(self.run_root)

  def test_get_invalid_state_does_not_overlay_saved_clips(self) -> None:
    save_clip_lab_state(
      self.run_root,
      {
        "schema_version": 1,
        "stage": "clip_lab_state",
        "candidate_manifest_sha256": self._manifest_sha(),
        "updated_at": "2026-06-26T00:00:00Z",
        "clips": {
          "candidate_review_clip_000001": {
            "clip_version": True,
            "review_status": "accepted",
            "reviewer_tags": ["should not appear"],
          }
        },
      },
    )

    view = build_clip_lab_view(self.run_root, run_id="run-1")

    self.assertTrue(view["invalid_state"])
    self.assertIsNotNone(view["invalid_state_reason"])
    clip = next(row for row in view["clips"] if row["clip_id"] == "candidate_review_clip_000001")
    self.assertEqual(clip["review_status"], "unresolved")
    self.assertEqual(clip["reviewer_tags"], [])

  def test_qc_scores_prefer_canonical_fields(self) -> None:
    artifacts = self.run_root / "artifacts"
    transcript_qc = {
      "schema_version": 1,
      "stage": "transcript_qc",
      "clips": [
        {
          "clip_id": "candidate_review_clip_000001",
          "transcript_match_score": 95,
          "ctc_min_span_score": 0.50,
        }
      ],
    }
    speaker_purity = {
      "schema_version": 1,
      "stage": "speaker_purity",
      "clips": [
        {
          "clip_id": "candidate_review_clip_000001",
          "speaker_check_score": 88,
          "min_window_similarity": 0.40,
        }
      ],
    }
    (artifacts / "transcript_qc.json").write_text(json.dumps(transcript_qc), encoding="utf-8")
    (artifacts / "speaker_purity.json").write_text(json.dumps(speaker_purity), encoding="utf-8")

    view = build_clip_lab_view(self.run_root, run_id="run-1")
    clip = next(row for row in view["clips"] if row["clip_id"] == "candidate_review_clip_000001")
    self.assertEqual(clip["transcript_match"], 95.0)
    self.assertEqual(clip["speaker_check"], 88.0)


if __name__ == "__main__":
  unittest.main()
