from __future__ import annotations

import json
import shutil
import tempfile
import threading
import unittest
import wave
from pathlib import Path
from unittest.mock import patch

import numpy as np

from speechcraft_dataset.clip_lab_coordination import (
    CANDIDATE_PROMOTE_BACKUP_REL,
    CANDIDATE_STAGE_PARENT_REL,
    CLIP_LAB_PEAKS_REL,
    CLIP_LAB_RENDERS_REL,
    CLIP_LAB_STATE_ARCHIVE_REL,
    CandidatePromotionRecoveryRequiredError,
    allocate_candidate_stage_root,
    assemble_candidate_review_clips_locked,
    archive_clip_lab_state,
    clear_clip_lab_render_caches,
    clip_lab_run_lock,
    finalize_candidate_regeneration,
    manifest_sha256,
    promote_staged_candidate_artifacts,
    validate_staged_candidate_artifacts,
    CandidatePromotionError,
)
from speechcraft_dataset.io import sha256_file
from speechcraft_dataset.qc_artifacts import clear_downstream_after_candidate_regeneration

SAMPLE_RATE = 16000


def _write_pcm16_mono(path: Path, samples: list[int], sample_rate: int = SAMPLE_RATE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(np.asarray(samples, dtype=np.int16).tobytes())


def _install_candidate_artifacts(
    root: Path,
    *,
    clip_id: str = "clip-1",
    samples: list[int] | None = None,
    hash_format: str = "prefixed",
) -> tuple[Path, str]:
    if samples is None:
        samples = [0, 100, -100, 200]
    artifacts = root / "artifacts"
    clips_dir = artifacts / "candidate_review_clips"
    clips_dir.mkdir(parents=True, exist_ok=True)
    wav_path = clips_dir / f"{clip_id}.wav"
    _write_pcm16_mono(wav_path, samples)
    audio_sha256 = sha256_file(wav_path)
    if hash_format == "raw":
        audio_sha256 = audio_sha256[7:]
    rel_audio_path = f"artifacts/candidate_review_clips/{clip_id}.wav"
    manifest = [
        {
            "id": clip_id,
            "audio_path": rel_audio_path,
            "audio_sha256": audio_sha256,
            "audio_hash": audio_sha256,
        }
    ]
    manifest_path = artifacts / "candidate_review_manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    (artifacts / "candidate_review_rejected.json").write_text("[]", encoding="utf-8")
    (artifacts / "candidate_review_summary.json").write_text("{}", encoding="utf-8")
    return manifest_path, audio_sha256


def _fake_assemble_factory(*, samples: list[int] | None = None, hash_format: str = "prefixed"):
    def _fake_assemble(run_root: Path, config: dict, *, artifact_root: Path | None = None) -> dict[str, object]:
        destination = artifact_root or run_root
        _install_candidate_artifacts(destination, samples=samples, hash_format=hash_format)
        return {"candidate_review_clips": 1}

    return _fake_assemble


class ClipLabCoordinationTests(unittest.TestCase):
    def test_clear_clip_lab_render_caches_removes_only_render_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True)
            (artifacts / "clip_lab_state.json").write_text("{}", encoding="utf-8")
            renders = run_root / CLIP_LAB_RENDERS_REL
            peaks = run_root / CLIP_LAB_PEAKS_REL
            renders.mkdir(parents=True)
            peaks.mkdir(parents=True)
            (renders / "clip.wav").write_bytes(b"wav")
            (peaks / "peaks.json").write_text("{}", encoding="utf-8")

            clear_clip_lab_render_caches(run_root)

            self.assertTrue((artifacts / "clip_lab_state.json").exists())
            self.assertFalse(renders.exists())
            self.assertFalse(peaks.exists())

    def test_clear_downstream_after_candidate_regeneration_preserves_clip_lab_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True)
            (artifacts / "clip_lab_state.json").write_text("{}", encoding="utf-8")

            clear_downstream_after_candidate_regeneration(run_root)

            self.assertTrue((artifacts / "clip_lab_state.json").exists())

    def test_concurrent_staging_directories_are_isolated(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            barrier = threading.Barrier(2)
            stage_dirs: list[Path] = []
            errors: list[BaseException] = []

            def worker(worker_id: int) -> None:
                try:
                    stage = allocate_candidate_stage_root(run_root)
                    stage_dirs.append(stage)
                    (stage / f"worker-{worker_id}.txt").write_text(str(worker_id), encoding="utf-8")
                    barrier.wait(timeout=2)
                    self.assertEqual((stage / f"worker-{worker_id}.txt").read_text(encoding="utf-8"), str(worker_id))
                except BaseException as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(worker_id,)) for worker_id in (1, 2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(errors, [])
            self.assertEqual(len(stage_dirs), 2)
            self.assertNotEqual(stage_dirs[0], stage_dirs[1])

    def test_failed_locked_assembly_preserves_live_candidate_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True)
            manifest_path, _ = _install_candidate_artifacts(run_root, samples=[1, 2, 3, 4])
            live_wav = artifacts / "candidate_review_clips" / "clip-1.wav"
            live_wav_bytes = live_wav.read_bytes()
            live_manifest_bytes = manifest_path.read_bytes()
            state_payload = {"schema_version": 1, "clips": {"clip-1": {"clip_version": 1}}}
            (artifacts / "clip_lab_state.json").write_text(json.dumps(state_payload), encoding="utf-8")

            with patch(
                "speechcraft_dataset.assembly.assemble_candidate_review_clips",
                side_effect=RuntimeError("assembly failed"),
            ):
                with self.assertRaises(RuntimeError):
                    assemble_candidate_review_clips_locked(run_root, {"analysis_sample_rate": SAMPLE_RATE})

            self.assertEqual(live_wav.read_bytes(), live_wav_bytes)
            self.assertEqual(manifest_path.read_bytes(), live_manifest_bytes)
            self.assertTrue((artifacts / "clip_lab_state.json").exists())
            self.assertFalse(any((run_root / CANDIDATE_STAGE_PARENT_REL).glob("*")))

    def test_staging_validation_failure_preserves_live_candidate_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            manifest_path, _ = _install_candidate_artifacts(run_root, samples=[10, 20, 30])
            live_wav_bytes = (run_root / "artifacts/candidate_review_clips/clip-1.wav").read_bytes()
            live_manifest_bytes = manifest_path.read_bytes()

            def _fake_bad_stage(run_root: Path, config: dict, *, artifact_root: Path | None = None) -> dict[str, object]:
                destination = artifact_root or run_root
                staged_manifest_path, _ = _install_candidate_artifacts(destination, samples=[40, 50, 60])
                manifest = json.loads(staged_manifest_path.read_text(encoding="utf-8"))
                manifest[0]["audio_sha256"] = "sha256:" + ("0" * 64)
                staged_manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                return {"candidate_review_clips": 1}

            with patch("speechcraft_dataset.assembly.assemble_candidate_review_clips", side_effect=_fake_bad_stage):
                with self.assertRaises(ValueError):
                    assemble_candidate_review_clips_locked(run_root, {"analysis_sample_rate": SAMPLE_RATE})

            self.assertEqual(
                (run_root / "artifacts/candidate_review_clips/clip-1.wav").read_bytes(),
                live_wav_bytes,
            )
            self.assertEqual(manifest_path.read_bytes(), live_manifest_bytes)

    def test_validate_staged_candidate_artifacts_accepts_prefixed_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            stage_root = Path(temp_dir_raw)
            _install_candidate_artifacts(stage_root, hash_format="prefixed")
            validate_staged_candidate_artifacts(stage_root)

    def test_validate_staged_candidate_artifacts_accepts_matching_raw_and_prefixed_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            stage_root = Path(temp_dir_raw)
            manifest_path, prefixed = _install_candidate_artifacts(stage_root, hash_format="prefixed")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest[0]["audio_sha256"] = prefixed
            manifest[0]["audio_hash"] = prefixed[7:]
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            validate_staged_candidate_artifacts(stage_root)

    def test_successful_changed_regeneration_archives_state_and_clears_caches(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True)
            _install_candidate_artifacts(run_root, samples=[1, 2, 3])
            state_payload = {"schema_version": 1, "clips": {"clip-1": {"clip_version": 1}}}
            (artifacts / "clip_lab_state.json").write_text(json.dumps(state_payload), encoding="utf-8")
            renders = run_root / CLIP_LAB_RENDERS_REL
            renders.mkdir(parents=True)
            (renders / "clip.wav").write_bytes(b"wav")

            with patch(
                "speechcraft_dataset.assembly.assemble_candidate_review_clips",
                side_effect=_fake_assemble_factory(samples=[9, 8, 7, 6]),
            ):
                summary = assemble_candidate_review_clips_locked(run_root, {"analysis_sample_rate": SAMPLE_RATE})

            self.assertEqual(summary["candidate_review_clips"], 1)
            self.assertFalse((artifacts / "clip_lab_state.json").exists())
            archive_files = list((run_root / CLIP_LAB_STATE_ARCHIVE_REL).glob("*/*.json"))
            self.assertEqual(len(archive_files), 1)
            self.assertEqual(json.loads(archive_files[0].read_text(encoding="utf-8")), state_payload)
            self.assertFalse(renders.exists())

    def test_successful_unchanged_regeneration_preserves_state_and_caches(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True)
            _install_candidate_artifacts(run_root, samples=[5, 6, 7, 8])
            state_payload = {"schema_version": 1, "clips": {"clip-1": {"clip_version": 3}}}
            (artifacts / "clip_lab_state.json").write_text(json.dumps(state_payload), encoding="utf-8")
            renders = run_root / CLIP_LAB_RENDERS_REL
            peaks = run_root / CLIP_LAB_PEAKS_REL
            renders.mkdir(parents=True)
            peaks.mkdir(parents=True)
            (renders / "clip.wav").write_bytes(b"wav")
            (peaks / "peaks.json").write_text("{}", encoding="utf-8")

            with patch(
                "speechcraft_dataset.assembly.assemble_candidate_review_clips",
                side_effect=_fake_assemble_factory(samples=[5, 6, 7, 8]),
            ):
                assemble_candidate_review_clips_locked(run_root, {"analysis_sample_rate": SAMPLE_RATE})

            self.assertTrue((artifacts / "clip_lab_state.json").exists())
            self.assertEqual(
                json.loads((artifacts / "clip_lab_state.json").read_text(encoding="utf-8")),
                state_payload,
            )
            self.assertTrue((renders / "clip.wav").exists())
            self.assertTrue((peaks / "peaks.json").exists())
            self.assertFalse((run_root / CLIP_LAB_STATE_ARCHIVE_REL).exists())

    def test_finalize_candidate_regeneration_archives_only_when_manifest_changed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True)
            (artifacts / "clip_lab_state.json").write_text("{}", encoding="utf-8")
            renders = run_root / CLIP_LAB_RENDERS_REL
            renders.mkdir(parents=True)

            finalize_candidate_regeneration(
                run_root,
                previous_manifest_sha256="abc123",
                new_manifest_sha256="abc123",
            )
            self.assertTrue((artifacts / "clip_lab_state.json").exists())
            self.assertTrue(renders.exists())

            finalize_candidate_regeneration(
                run_root,
                previous_manifest_sha256="abc123",
                new_manifest_sha256="def456",
            )
            self.assertFalse((artifacts / "clip_lab_state.json").exists())
            archive_files = list((run_root / CLIP_LAB_STATE_ARCHIVE_REL / "abc123").glob("*.json"))
            self.assertEqual(len(archive_files), 1)
            self.assertFalse(renders.exists())

    def test_archive_clip_lab_state_allows_multiple_archives_for_same_manifest_hash(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True)
            state_path = artifacts / "clip_lab_state.json"
            state_path.write_text('{"generation": 1}', encoding="utf-8")
            first = archive_clip_lab_state(run_root, previous_manifest_sha256="same-hash")
            self.assertIsNotNone(first)
            state_path.write_text('{"generation": 2}', encoding="utf-8")
            second = archive_clip_lab_state(run_root, previous_manifest_sha256="same-hash")
            self.assertIsNotNone(second)
            self.assertNotEqual(first, second)
            archives = list((run_root / CLIP_LAB_STATE_ARCHIVE_REL / "same-hash").glob("*.json"))
            self.assertEqual(len(archives), 2)
            payloads = {path.read_text(encoding="utf-8") for path in archives}
            self.assertEqual(payloads, {'{"generation": 1}', '{"generation": 2}'})

    def test_archive_clip_lab_state_is_noop_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            self.assertIsNone(archive_clip_lab_state(run_root, previous_manifest_sha256="abc123"))

    def test_validate_staged_candidate_artifacts_rejects_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            stage_root = Path(temp_dir_raw)
            manifest_path, _ = _install_candidate_artifacts(stage_root)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest[0]["audio_sha256"] = "sha256:" + ("f" * 64)
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaises(ValueError):
                validate_staged_candidate_artifacts(stage_root)

    def test_promote_staged_candidate_artifacts_replaces_live_set(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            _install_candidate_artifacts(run_root, samples=[1, 1, 1])
            stage_root = run_root / "stage"
            _install_candidate_artifacts(stage_root, samples=[9, 9, 9])
            staged_wav_path = stage_root / "artifacts/candidate_review_clips/clip-1.wav"
            expected_bytes = staged_wav_path.read_bytes()
            promote_staged_candidate_artifacts(stage_root, run_root)
            live_wav = run_root / "artifacts/candidate_review_clips/clip-1.wav"
            self.assertEqual(live_wav.read_bytes(), expected_bytes)
            self.assertEqual(
                manifest_sha256(run_root / "artifacts/candidate_review_manifest.json"),
                manifest_sha256(stage_root / "artifacts/candidate_review_manifest.json"),
            )

    def test_promotion_rollback_failure_preserves_backup_for_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            _install_candidate_artifacts(run_root, samples=[1, 2, 3])
            stage_root = run_root / "stage"
            _install_candidate_artifacts(stage_root, samples=[4, 5, 6])
            backup = run_root / CANDIDATE_PROMOTE_BACKUP_REL
            real_move = shutil.move

            def flaky_move(src: str, dst: str) -> str:
                if "stage" in str(src) and Path(src).name == "candidate_review_clips":
                    raise OSError("promotion move failed")
                return real_move(src, dst)

            with (
                patch("speechcraft_dataset.clip_lab_coordination.shutil.move", side_effect=flaky_move),
                patch(
                    "speechcraft_dataset.clip_lab_coordination._restore_live_candidate_artifacts",
                    side_effect=OSError("rollback failed"),
                ),
            ):
                with self.assertRaises(CandidatePromotionRecoveryRequiredError):
                    promote_staged_candidate_artifacts(stage_root, run_root)
            self.assertTrue(backup.exists())
            self.assertTrue((backup / "candidate_review_clips" / "clip-1.wav").exists())

    def test_second_promotion_blocked_while_recovery_backup_exists(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            _install_candidate_artifacts(run_root, samples=[1, 2, 3])
            stage_root = run_root / "stage"
            _install_candidate_artifacts(stage_root, samples=[4, 5, 6])
            backup = run_root / CANDIDATE_PROMOTE_BACKUP_REL
            real_move = shutil.move
            backup.mkdir(parents=True)
            shutil.copytree(
                run_root / "artifacts/candidate_review_clips",
                backup / "candidate_review_clips",
            )
            backup_manifest = run_root / "artifacts/candidate_review_manifest.json"
            shutil.copy2(backup_manifest, backup / "candidate_review_manifest.json")
            backup_wav_bytes = (backup / "candidate_review_clips/clip-1.wav").read_bytes()
            live_wav_bytes = (run_root / "artifacts/candidate_review_clips/clip-1.wav").read_bytes()

            stage_root2 = run_root / "stage2"
            _install_candidate_artifacts(stage_root2, samples=[9, 9, 9])

            with self.assertRaises(CandidatePromotionRecoveryRequiredError):
                promote_staged_candidate_artifacts(stage_root2, run_root)

            self.assertEqual(
                (run_root / "artifacts/candidate_review_clips/clip-1.wav").read_bytes(),
                live_wav_bytes,
            )
            self.assertEqual((backup / "candidate_review_clips/clip-1.wav").read_bytes(), backup_wav_bytes)

            with patch(
                "speechcraft_dataset.assembly.assemble_candidate_review_clips",
                side_effect=_fake_assemble_factory(samples=[8, 8, 8]),
            ):
                with self.assertRaises(CandidatePromotionRecoveryRequiredError):
                    assemble_candidate_review_clips_locked(run_root, {"analysis_sample_rate": SAMPLE_RATE})

    def test_failed_promotion_with_successful_rollback_cleans_stage_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            _install_candidate_artifacts(run_root, samples=[1, 2, 3])
            real_move = shutil.move

            def flaky_move(src: str, dst: str) -> str:
                src_path = Path(src)
                dst_path = Path(dst)
                if (
                    "_candidate_review_stage" in str(src_path)
                    and dst_path.name == "candidate_review_clips"
                    and "_candidate_review_promote_backup" not in str(dst_path)
                ):
                    raise OSError("promotion move failed")
                return real_move(src, dst)

            with patch(
                "speechcraft_dataset.assembly.assemble_candidate_review_clips",
                side_effect=_fake_assemble_factory(samples=[4, 5, 6]),
            ), patch("speechcraft_dataset.clip_lab_coordination.shutil.move", side_effect=flaky_move):
                with self.assertRaises(CandidatePromotionError) as raised:
                    assemble_candidate_review_clips_locked(run_root, {"analysis_sample_rate": SAMPLE_RATE})
                self.assertNotIsInstance(raised.exception, CandidatePromotionRecoveryRequiredError)

            self.assertFalse(any((run_root / CANDIDATE_STAGE_PARENT_REL).glob("*")))

    def test_recovery_required_preserves_stage_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            _install_candidate_artifacts(run_root, samples=[1, 2, 3])
            real_move = shutil.move

            def flaky_move(src: str, dst: str) -> str:
                src_path = Path(src)
                dst_path = Path(dst)
                if (
                    "_candidate_review_stage" in str(src_path)
                    and dst_path.name == "candidate_review_clips"
                    and "_candidate_review_promote_backup" not in str(dst_path)
                ):
                    raise OSError("promotion move failed")
                return real_move(src, dst)

            with patch(
                "speechcraft_dataset.assembly.assemble_candidate_review_clips",
                side_effect=_fake_assemble_factory(samples=[4, 5, 6]),
            ), patch("speechcraft_dataset.clip_lab_coordination.shutil.move", side_effect=flaky_move), patch(
                "speechcraft_dataset.clip_lab_coordination._restore_live_candidate_artifacts",
                side_effect=OSError("rollback failed"),
            ):
                with self.assertRaises(CandidatePromotionRecoveryRequiredError):
                    assemble_candidate_review_clips_locked(run_root, {"analysis_sample_rate": SAMPLE_RATE})

            stage_dirs = list((run_root / CANDIDATE_STAGE_PARENT_REL).glob("*"))
            self.assertEqual(len(stage_dirs), 1)
            self.assertTrue((stage_dirs[0] / "artifacts/candidate_review_clips/clip-1.wav").exists())

    def test_promotion_rollback_restores_absence_when_no_prior_artifacts(self) -> None:
        from speechcraft_dataset.clip_lab_coordination import _atomic_copy_file as real_copy

        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            (run_root / "artifacts").mkdir(parents=True)
            stage_root = run_root / "stage"
            _install_candidate_artifacts(stage_root, samples=[4, 5, 6])

            def flaky_copy(source: Path, target: Path) -> None:
                if target.name == "candidate_review_manifest.json":
                    raise OSError("manifest copy failed")
                real_copy(source, target)

            with patch(
                "speechcraft_dataset.clip_lab_coordination._atomic_copy_file",
                side_effect=flaky_copy,
            ):
                with self.assertRaises(CandidatePromotionError) as raised:
                    promote_staged_candidate_artifacts(stage_root, run_root)
                self.assertNotIsInstance(raised.exception, CandidatePromotionRecoveryRequiredError)

            artifacts = run_root / "artifacts"
            self.assertFalse((artifacts / "candidate_review_clips").exists())
            self.assertFalse((artifacts / "candidate_review_manifest.json").exists())
            self.assertFalse((artifacts / "candidate_review_rejected.json").exists())
            self.assertFalse((artifacts / "candidate_review_summary.json").exists())

    def test_clip_lab_run_lock_blocks_second_holder(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            with clip_lab_run_lock(run_root):
                with self.assertRaises(TimeoutError):
                    with clip_lab_run_lock(run_root, timeout=0):
                        pass


if __name__ == "__main__":
    unittest.main()
