from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import unittest
import wave
from pathlib import Path
from unittest.mock import patch

import numpy as np

from app.clip_lab_audio import (
    render_audio_ops_to_cache_from_bytes,
)
from app.clip_lab_audio_ops import (
    ClipLabPeaksCacheMissingError,
    _RenderJob,
    _finalize_render,
    _mark_render_failed_if_current,
    append_clip_audio_operation,
    audio_view_fields,
    load_revision_peaks_payload,
)
from app.clip_lab_state import (
    StaleManifestError,
    compute_manifest_sha256,
    load_clip_lab_state,
    patch_clip_lab_clip,
)

SAMPLE_RATE = 16000


def _write_pcm16_mono(path: Path, samples: list[int], sample_rate: int = SAMPLE_RATE) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    array = np.asarray(samples, dtype=np.int16)
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(array.tobytes())
    return path.read_bytes()


def _install_run(run_root: Path, *, clip_id: str = "clip-1", samples: list[int] | None = None) -> tuple[str, str, bytes]:
    if samples is None:
        samples = list(range(100, 160))
    artifacts = run_root / "artifacts"
    clips_dir = artifacts / "candidate_review_clips"
    wav_path = clips_dir / f"{clip_id}.wav"
    wav_bytes = _write_pcm16_mono(wav_path, samples)
    source_sha = hashlib.sha256(wav_bytes).hexdigest()
    manifest = [
        {
            "id": clip_id,
            "audio_path": f"artifacts/candidate_review_clips/{clip_id}.wav",
            "audio_sha256": source_sha,
            "audio_hash": source_sha,
            "sample_rate": SAMPLE_RATE,
            "duration_samples": len(samples),
            "duration_sec": round(len(samples) / SAMPLE_RATE, 6),
            "training_text": "hello",
        }
    ]
    (artifacts / "candidate_review_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    manifest_sha = compute_manifest_sha256(artifacts / "candidate_review_manifest.json")
    return clip_id, manifest_sha, wav_bytes


class ClipLabAudioOpsLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.run_root = Path(self.temp_dir.name)
        self.clip_id, self.manifest_sha, self.source_wav_bytes = _install_run(self.run_root)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _append_delete(self, *, version: int = 0) -> dict:
        return append_clip_audio_operation(
            self.run_root,
            run_id="run-1",
            clip_id=self.clip_id,
            expected_manifest_sha256=self.manifest_sha,
            expected_clip_version=version,
            operation={"kind": "delete_range", "start_sample": 5, "end_sample": 15},
        )

    def test_finalize_render_succeeds_after_tag_only_clip_version_bump(self) -> None:
        import app.clip_lab_audio_ops as ops_module

        render_started = threading.Event()
        allow_finalize = threading.Event()
        real_render = ops_module._perform_sync_render
        real_finalize = ops_module._finalize_render

        def delayed_render(job: _RenderJob) -> str:
            rendered_sha = real_render(job)
            render_started.set()
            allow_finalize.wait(timeout=5)
            return rendered_sha

        def gated_finalize(run_root: Path, clip_id: str, *, job: _RenderJob, rendered_audio_sha256: str) -> bool:
            allow_finalize.wait(timeout=5)
            return real_finalize(run_root, clip_id, job=job, rendered_audio_sha256=rendered_audio_sha256)

        errors: list[BaseException] = []
        result: dict | None = None

        def worker() -> None:
            nonlocal result
            try:
                with patch.object(ops_module, "_perform_sync_render", side_effect=delayed_render), patch.object(
                    ops_module, "_finalize_render", side_effect=gated_finalize
                ):
                    result = self._append_delete(version=0)
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=worker)
        thread.start()
        self.assertTrue(render_started.wait(timeout=5))
        patch_clip_lab_clip(
            self.run_root,
            self.clip_id,
            run_id="run-1",
            expected_manifest_sha256=self.manifest_sha,
            expected_clip_version=1,
            reviewer_tags=["tagged-during-render"],
        )
        allow_finalize.set()
        thread.join(timeout=10)
        self.assertEqual(errors, [])
        assert result is not None
        self.assertEqual(result["render_status"], "ready")
        self.assertIsNotNone(result["audio_revision_hash"])
        state = load_clip_lab_state(self.run_root)
        assert state is not None
        self.assertEqual(state["clips"][self.clip_id]["audio_edit"]["render_status"], "ready")

    def test_stale_render_failure_does_not_mark_newer_revision_failed(self) -> None:
        first = self._append_delete(version=0)
        revision_a = first["audio_revision_hash"]
        assert isinstance(revision_a, str)
        second = append_clip_audio_operation(
            self.run_root,
            run_id="run-1",
            clip_id=self.clip_id,
            expected_manifest_sha256=self.manifest_sha,
            expected_clip_version=first["clip_version"],
            operation={"kind": "delete_range", "start_sample": 20, "end_sample": 25},
        )
        revision_b = second["audio_revision_hash"]
        assert isinstance(revision_b, str)
        self.assertNotEqual(revision_a, revision_b)

        job_a = _RenderJob(
            run_root=self.run_root,
            clip_id=self.clip_id,
            source_wav_bytes=self.source_wav_bytes,
            source_audio_sha256=first["source_audio_sha256"],
            expected_manifest_sha256=self.manifest_sha,
            sample_rate=SAMPLE_RATE,
            ops=[{"kind": "delete_range", "start_sample": 5, "end_sample": 15}],
            revision_hash=revision_a,
        )
        _mark_render_failed_if_current(self.run_root, self.clip_id, job=job_a)

        state = load_clip_lab_state(self.run_root)
        assert state is not None
        audio_edit = state["clips"][self.clip_id]["audio_edit"]
        self.assertEqual(audio_edit["audio_revision_hash"], revision_b)
        self.assertEqual(audio_edit["render_status"], "ready")

    def test_render_uses_captured_source_bytes_after_candidate_file_changes(self) -> None:
        from app.clip_lab_audio_ops import _perform_sync_render

        job = _RenderJob(
            run_root=self.run_root,
            clip_id=self.clip_id,
            source_wav_bytes=self.source_wav_bytes,
            source_audio_sha256=hashlib.sha256(self.source_wav_bytes).hexdigest(),
            expected_manifest_sha256=self.manifest_sha,
            sample_rate=SAMPLE_RATE,
            ops=[{"kind": "delete_range", "start_sample": 5, "end_sample": 15}],
            revision_hash="deadbeef" * 8,
        )
        replacement_samples = [999] * 60
        wav_path = self.run_root / "artifacts/candidate_review_clips" / f"{self.clip_id}.wav"
        _write_pcm16_mono(wav_path, replacement_samples)

        rendered_sha = _perform_sync_render(job)
        cache_path = self.run_root / "artifacts/clip_lab_renders" / self.clip_id / f"{job.revision_hash}.wav"
        self.assertTrue(cache_path.is_file())
        self.assertEqual(hashlib.sha256(cache_path.read_bytes()).hexdigest(), rendered_sha)
        from app.clip_lab_audio import apply_audio_ops, load_pcm16_mono_wav

        rendered_samples, _ = load_pcm16_mono_wav(cache_path)
        expected_from_a = apply_audio_ops(
            np.asarray(list(range(100, 160)), dtype=np.int16),
            job.ops,
            sample_rate=SAMPLE_RATE,
        )
        np.testing.assert_array_equal(rendered_samples, expected_from_a)

    def test_concurrent_same_revision_renders_do_not_collide(self) -> None:
        run_root = self.run_root
        cache_path = run_root / "artifacts/clip_lab_renders" / self.clip_id / ("ab" * 32 + ".wav")
        ops = [{"kind": "delete_range", "start_sample": 5, "end_sample": 15}]
        source_sha = hashlib.sha256(self.source_wav_bytes).hexdigest()
        errors: list[BaseException] = []
        results: list[str] = []

        def worker() -> None:
            try:
                _, _, rendered_sha = render_audio_ops_to_cache_from_bytes(
                    source_wav_bytes=self.source_wav_bytes,
                    ops=ops,
                    cache_path=cache_path,
                    source_audio_sha256=source_sha,
                )
                results.append(rendered_sha)
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(set(results)), 1)
        self.assertTrue(cache_path.is_file())

    def test_pending_and_failed_views_use_source_duration(self) -> None:
        manifest_row = json.loads(
            (self.run_root / "artifacts/candidate_review_manifest.json").read_text(encoding="utf-8")
        )[0]
        source_duration = round(60 / SAMPLE_RATE, 6)
        clip_entry = {
            "clip_version": 1,
            "audio_edit": {
                "schema_version": 1,
                "source_audio_sha256": manifest_row["audio_sha256"],
                "source_sample_rate_hz": SAMPLE_RATE,
                "ops": [{"kind": "delete_range", "start_sample": 5, "end_sample": 15}],
                "redo_ops": [],
                "audio_revision_hash": "rev" * 16,
                "rendered_audio_sha256": None,
                "render_status": "pending",
            },
        }
        pending_view = audio_view_fields(run_id="run-1", manifest_row=manifest_row, clip_entry=clip_entry)
        self.assertEqual(pending_view["effective_audio_kind"], "candidate_original")
        self.assertEqual(pending_view["current_duration_sec"], source_duration)

        clip_entry["audio_edit"]["render_status"] = "failed"
        failed_view = audio_view_fields(run_id="run-1", manifest_row=manifest_row, clip_entry=clip_entry)
        self.assertEqual(failed_view["current_duration_sec"], source_duration)

        clip_entry["audio_edit"]["render_status"] = "ready"
        clip_entry["audio_edit"]["rendered_audio_sha256"] = "abc"
        ready_view = audio_view_fields(run_id="run-1", manifest_row=manifest_row, clip_entry=clip_entry)
        self.assertEqual(ready_view["effective_audio_kind"], "rendered_revision")
        self.assertLess(ready_view["current_duration_sec"], source_duration)

    def test_ready_rendered_peaks_missing_raises_without_rendering(self) -> None:
        edited = self._append_delete(version=0)
        revision_key = edited["effective_audio_revision_key"]
        peaks_path = self.run_root / "artifacts/clip_lab_peaks" / f"{revision_key}.json"
        if peaks_path.exists():
            peaks_path.unlink()
        manifest_row = json.loads(
            (self.run_root / "artifacts/candidate_review_manifest.json").read_text(encoding="utf-8")
        )[0]
        state = load_clip_lab_state(self.run_root)
        clip_entry = state["clips"][self.clip_id]
        with patch("app.clip_lab_audio_ops.render_or_reuse_audio_revision_from_bytes") as render_mock:
            with self.assertRaises(ClipLabPeaksCacheMissingError):
                load_revision_peaks_payload(
                    self.run_root,
                    clip_id=self.clip_id,
                    revision_key=revision_key,
                    manifest_row=manifest_row,
                    clip_entry=clip_entry,
                )
        render_mock.assert_not_called()

    def test_finalize_failure_raises_stale_manifest(self) -> None:
        with patch("app.clip_lab_audio_ops._finalize_render", return_value=False):
            with self.assertRaises(StaleManifestError):
                self._append_delete(version=0)

    def test_source_peaks_use_captured_bytes_after_file_swap(self) -> None:
        import app.clip_lab_audio_ops as ops_module

        manifest_row = json.loads(
            (self.run_root / "artifacts/candidate_review_manifest.json").read_text(encoding="utf-8")
        )[0]
        source_sha = manifest_row["audio_sha256"]
        allow_compute = threading.Event()
        real_loader = ops_module.load_source_peaks_payload_from_bytes

        def delayed_loader(**kwargs):
            allow_compute.wait(timeout=5)
            return real_loader(**kwargs)

        errors: list[BaseException] = []
        payload_holder: dict[str, object] = {}

        def worker() -> None:
            try:
                with patch.object(ops_module, "load_source_peaks_payload_from_bytes", side_effect=delayed_loader):
                    payload_holder["payload"] = ops_module.load_revision_peaks_payload(
                        self.run_root,
                        clip_id=self.clip_id,
                        revision_key=source_sha,
                        manifest_row=manifest_row,
                        clip_entry={},
                        source_wav_bytes=self.source_wav_bytes,
                    )
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=worker)
        thread.start()
        _write_pcm16_mono(
            self.run_root / "artifacts/candidate_review_clips" / f"{self.clip_id}.wav",
            [999] * 60,
        )
        allow_compute.set()
        thread.join(timeout=10)
        self.assertEqual(errors, [])
        payload = payload_holder["payload"]
        assert isinstance(payload, dict)
        self.assertEqual(payload["revision_key"], source_sha)
        self.assertEqual(payload["bins"], 960)


if __name__ == "__main__":
    unittest.main()
