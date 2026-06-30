from __future__ import annotations

import hashlib
import importlib.metadata
import json
import tempfile
import unittest
import wave
from pathlib import Path

import numpy as np

from app.clip_lab_audio import (
    MAX_INSERT_SILENCE_SEC,
    PEAK_BIN_COUNT,
    ClipLabAudioValidationError,
    ClipLabSourceIdentityError,
    apply_audio_ops,
    atomic_publish_peaks_payload,
    build_peaks_payload,
    canonical_recipe_json,
    compute_audio_revision_hash,
    compute_waveform_peaks,
    load_pcm16_mono_wav,
    render_audio_ops_to_cache,
    render_audio_ops_to_wav,
    verify_source_wav_identity,
    write_pcm16_mono_wav,
)

SAMPLE_RATE = 16000


def _write_test_wav(path: Path, samples: list[int], sample_rate: int = SAMPLE_RATE) -> None:
    write_pcm16_mono_wav(path, np.asarray(samples, dtype=np.int16), sample_rate)


class ClipLabAudioTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.run_root = Path(self.temp_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_backend_declares_numpy_dependency(self) -> None:
        requires = importlib.metadata.requires("speechcraft-backend") or []
        self.assertTrue(any(requirement.startswith("numpy") for requirement in requires))

    def test_delete_range_produces_expected_frames(self) -> None:
        samples = np.arange(10, dtype=np.int16)
        result = apply_audio_ops(
            samples,
            [{"kind": "delete_range", "start_sample": 2, "end_sample": 5}],
            sample_rate=SAMPLE_RATE,
        )
        np.testing.assert_array_equal(result, np.array([0, 1, 5, 6, 7, 8, 9], dtype=np.int16))

    def test_insert_silence_inserts_zero_frames(self) -> None:
        samples = np.array([1, 2, 3], dtype=np.int16)
        result = apply_audio_ops(
            samples,
            [{"kind": "insert_silence", "at_sample": 1, "duration_samples": 2}],
            sample_rate=SAMPLE_RATE,
        )
        np.testing.assert_array_equal(result, np.array([1, 0, 0, 2, 3], dtype=np.int16))

    def test_chained_ops_validate_post_previous_op_timeline(self) -> None:
        samples = np.arange(8, dtype=np.int16)
        ops = [
            {"kind": "delete_range", "start_sample": 1, "end_sample": 3},
            {"kind": "insert_silence", "at_sample": 2, "duration_samples": 2},
        ]
        result = apply_audio_ops(samples, ops, sample_rate=SAMPLE_RATE)
        np.testing.assert_array_equal(result, np.array([0, 3, 0, 0, 4, 5, 6, 7], dtype=np.int16))

    def test_delete_range_rejects_out_of_bounds(self) -> None:
        samples = np.arange(4, dtype=np.int16)
        with self.assertRaises(ClipLabAudioValidationError):
            apply_audio_ops(
                samples,
                [{"kind": "delete_range", "start_sample": 0, "end_sample": 5}],
                sample_rate=SAMPLE_RATE,
            )

    def test_delete_range_rejects_unknown_keys(self) -> None:
        samples = np.arange(4, dtype=np.int16)
        with self.assertRaisesRegex(ClipLabAudioValidationError, "unexpected field"):
            apply_audio_ops(
                samples,
                [{"kind": "delete_range", "start_sample": 0, "end_sample": 1, "banana": True}],
                sample_rate=SAMPLE_RATE,
            )

    def test_delete_entire_clip_is_rejected(self) -> None:
        samples = np.arange(8, dtype=np.int16)
        with self.assertRaisesRegex(ClipLabAudioValidationError, "entire clip"):
            apply_audio_ops(
                samples,
                [{"kind": "delete_range", "start_sample": 0, "end_sample": 8}],
                sample_rate=SAMPLE_RATE,
            )

    def test_huge_insert_silence_is_rejected_before_allocation(self) -> None:
        samples = np.arange(4, dtype=np.int16)
        with self.assertRaisesRegex(ClipLabAudioValidationError, f"{MAX_INSERT_SILENCE_SEC:g} seconds"):
            apply_audio_ops(
                samples,
                [{"kind": "insert_silence", "at_sample": 0, "duration_samples": 2_147_483_647}],
                sample_rate=SAMPLE_RATE,
            )

    def test_revision_hash_depends_on_source_hash(self) -> None:
        ops = [{"kind": "delete_range", "start_sample": 0, "end_sample": 1}]
        hash_a = compute_audio_revision_hash(
            "a" * 64,
            ops,
            source_sample_count=4,
            sample_rate=SAMPLE_RATE,
        )
        hash_b = compute_audio_revision_hash(
            "b" * 64,
            ops,
            source_sample_count=4,
            sample_rate=SAMPLE_RATE,
        )
        self.assertNotEqual(hash_a, hash_b)
        self.assertEqual(
            hash_a,
            compute_audio_revision_hash(
                "a" * 64,
                ops,
                renderer_version="1",
                source_sample_count=4,
                sample_rate=SAMPLE_RATE,
            ),
        )

    def test_revision_hash_rejects_invalid_recipe_before_hashing(self) -> None:
        ops = [{"kind": "delete_range", "start_sample": 0, "end_sample": 8, "banana": True}]
        with self.assertRaises(ClipLabAudioValidationError):
            compute_audio_revision_hash(
                "a" * 64,
                ops,
                source_sample_count=8,
                sample_rate=SAMPLE_RATE,
            )

    def test_revision_hash_uses_canonical_recipe_json(self) -> None:
        ops = [{"kind": "insert_silence", "at_sample": 4, "duration_samples": 1600}]
        expected_recipe = canonical_recipe_json(ops)
        self.assertIn("schema_version", expected_recipe)
        self.assertIn('"schema_version":1', expected_recipe)
        self.assertIsNotNone(
            compute_audio_revision_hash(
                "c" * 64,
                ops,
                source_sample_count=8,
                sample_rate=SAMPLE_RATE,
            )
        )

    def test_render_audio_ops_round_trip_pcm16_mono(self) -> None:
        source_path = self.run_root / "source.wav"
        output_path = self.run_root / "rendered.wav"
        _write_test_wav(source_path, [100, 200, 300, 400, 500])

        rendered, sample_rate = render_audio_ops_to_wav(
            source_wav_path=source_path,
            ops=[{"kind": "delete_range", "start_sample": 1, "end_sample": 3}],
            output_path=output_path,
        )

        self.assertEqual(sample_rate, SAMPLE_RATE)
        np.testing.assert_array_equal(rendered, np.array([100, 400, 500], dtype=np.int16))
        reloaded, reloaded_rate = load_pcm16_mono_wav(output_path)
        self.assertEqual(reloaded_rate, SAMPLE_RATE)
        np.testing.assert_array_equal(reloaded, rendered)
        with wave.open(str(output_path), "rb") as handle:
            self.assertEqual(handle.getnchannels(), 1)
            self.assertEqual(handle.getsampwidth(), 2)
            self.assertEqual(handle.getcomptype(), "NONE")

    def test_compute_waveform_peaks_always_returns_960_bins(self) -> None:
        peaks = compute_waveform_peaks(np.array([0, 1000, -2000, 3000], dtype=np.int16))
        self.assertEqual(len(peaks), PEAK_BIN_COUNT)
        self.assertGreater(max(peaks), 0.0)
        self.assertLessEqual(max(peaks), 1.0)

    def test_compute_waveform_peaks_clamps_to_one(self) -> None:
        peaks = compute_waveform_peaks(np.array([-32768], dtype=np.int16))
        self.assertEqual(peaks[0], 1.0)

    def test_build_peaks_payload_reports_fixed_bin_count(self) -> None:
        payload = build_peaks_payload(
            revision_key="rev",
            samples=np.array([0, 1000], dtype=np.int16),
            sample_rate=SAMPLE_RATE,
        )
        self.assertEqual(payload["bins"], PEAK_BIN_COUNT)
        self.assertEqual(len(payload["peaks"]), PEAK_BIN_COUNT)

    def test_load_pcm16_mono_wav_rejects_stereo(self) -> None:
        stereo_path = self.run_root / "stereo.wav"
        with wave.open(str(stereo_path), "wb") as handle:
            handle.setnchannels(2)
            handle.setsampwidth(2)
            handle.setframerate(SAMPLE_RATE)
            handle.writeframes(np.zeros(4, dtype=np.int16).tobytes())
        with self.assertRaisesRegex(ClipLabAudioValidationError, "mono"):
            load_pcm16_mono_wav(stereo_path)

    def test_write_pcm16_mono_wav_rejects_non_1d_samples(self) -> None:
        with self.assertRaisesRegex(ClipLabAudioValidationError, "1-D"):
            write_pcm16_mono_wav(self.run_root / "bad.wav", np.zeros((2, 2), dtype=np.int16), SAMPLE_RATE)

    def test_write_pcm16_mono_wav_rejects_invalid_sample_rate(self) -> None:
        with self.assertRaisesRegex(ClipLabAudioValidationError, "sample_rate"):
            write_pcm16_mono_wav(self.run_root / "bad.wav", np.zeros(2, dtype=np.int16), 0)

    def test_verify_source_wav_identity_rejects_hash_mismatch(self) -> None:
        source_path = self.run_root / "source.wav"
        _write_test_wav(source_path, [1, 2, 3])
        with self.assertRaises(ClipLabSourceIdentityError):
            verify_source_wav_identity(source_path, "a" * 64)

    def test_verify_source_wav_identity_accepts_prefixed_digest(self) -> None:
        source_path = self.run_root / "source.wav"
        _write_test_wav(source_path, [1, 2, 3])
        digest = hashlib.sha256(source_path.read_bytes()).hexdigest()
        verify_source_wav_identity(source_path, f"sha256:{digest}")

    def test_render_audio_ops_refuses_source_hash_mismatch(self) -> None:
        source_path = self.run_root / "source.wav"
        output_path = self.run_root / "rendered.wav"
        _write_test_wav(source_path, [1, 2, 3, 4])
        with self.assertRaises(ClipLabSourceIdentityError):
            render_audio_ops_to_wav(
                source_wav_path=source_path,
                ops=[{"kind": "delete_range", "start_sample": 0, "end_sample": 1}],
                output_path=output_path,
                source_audio_sha256=hashlib.sha256(b"mismatch").hexdigest(),
            )

    def test_render_audio_ops_accepts_matching_source_hash(self) -> None:
        source_path = self.run_root / "source.wav"
        output_path = self.run_root / "rendered.wav"
        _write_test_wav(source_path, [1, 2, 3, 4])
        source_hash = hashlib.sha256(source_path.read_bytes()).hexdigest()
        render_audio_ops_to_wav(
            source_wav_path=source_path,
            ops=[{"kind": "delete_range", "start_sample": 0, "end_sample": 1}],
            output_path=output_path,
            source_audio_sha256=source_hash,
        )
        self.assertTrue(output_path.exists())

    def test_render_audio_ops_to_cache_publishes_atomically(self) -> None:
        source_path = self.run_root / "source.wav"
        cache_path = self.run_root / "renders" / "clip-1" / "revision.wav"
        _write_test_wav(source_path, [10, 20, 30, 40, 50])
        rendered, sample_rate, rendered_sha256 = render_audio_ops_to_cache(
            source_wav_path=source_path,
            ops=[{"kind": "delete_range", "start_sample": 0, "end_sample": 1}],
            cache_path=cache_path,
        )
        self.assertTrue(cache_path.exists())
        self.assertFalse(cache_path.with_suffix(cache_path.suffix + ".tmp").exists())
        self.assertEqual(len(rendered), 4)
        self.assertEqual(sample_rate, SAMPLE_RATE)
        self.assertEqual(rendered_sha256, hashlib.sha256(cache_path.read_bytes()).hexdigest())

    def test_atomic_publish_peaks_payload_writes_complete_json(self) -> None:
        peaks_path = self.run_root / "peaks" / "revision.json"
        payload = build_peaks_payload(
            revision_key="rev",
            samples=np.array([0, 1000], dtype=np.int16),
            sample_rate=SAMPLE_RATE,
        )
        atomic_publish_peaks_payload(peaks_path, payload)
        self.assertTrue(peaks_path.exists())
        self.assertFalse(peaks_path.with_suffix(peaks_path.suffix + ".tmp").exists())
        published = json.loads(peaks_path.read_text(encoding="utf-8"))
        self.assertEqual(published["revision_key"], "rev")
        self.assertEqual(len(published["peaks"]), PEAK_BIN_COUNT)


if __name__ == "__main__":
    unittest.main()
