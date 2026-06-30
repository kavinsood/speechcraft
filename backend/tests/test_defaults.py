from __future__ import annotations

import unittest

from app.defaults import (
    DATASET_PROCESSING_DEFAULTS,
    build_dataset_worker_config,
    build_slicer_config_overrides,
    resolve_asr_device_and_compute_type,
    resolve_mfa_models,
    resolve_whisper_model,
)


class DefaultsTests(unittest.TestCase):
    def test_build_slicer_config_rejects_hardcoded_overrides(self) -> None:
        config = build_slicer_config_overrides({"cutpoint_frame_ms": 99, "candidate_target_clip_sec": 5.0})
        self.assertEqual(config["cutpoint_frame_ms"], 10)
        self.assertEqual(config["candidate_target_clip_sec"], 5.0)

    def test_build_dataset_worker_config(self) -> None:
        config = build_dataset_worker_config(language="en", whisper_model_size="large-v3")

        self.assertEqual(config["max_processing_buffer_sec"], 29.5)
        self.assertEqual(config["processing_buffer_pad_sec"], 0.25)
        self.assertEqual(config["target_processing_chunk_sec"], 25.0)
        self.assertEqual(config["min_split_gap_sec"], 0.15)
        self.assertEqual(config["min_asr_mfa_buffer_sec"], 1.0)
        self.assertEqual(config["faster_whisper_beam_size"], 5)
        self.assertEqual(config["asr_condition_on_previous_text"], False)
        self.assertEqual(config["asr_word_timestamps"], False)

    def test_language_maps_mfa_models(self) -> None:
        self.assertEqual(resolve_mfa_models("en"), ("english_us_mfa", "english_mfa"))
        self.assertEqual(resolve_mfa_models("es"), ("spanish_mfa", "spanish_mfa"))
        self.assertEqual(resolve_mfa_models("auto"), ("english_us_mfa", "english_mfa"))

    def test_auto_language_uses_whisper_auto_detect(self) -> None:
        config = build_dataset_worker_config(language="auto", whisper_model_size="base")
        self.assertEqual(config["asr_language"], "auto")
        self.assertEqual(config["faster_whisper_model"], "base")

    def test_explicit_language_sets_asr_language(self) -> None:
        config = build_dataset_worker_config(language="fr", whisper_model_size="large-v3")
        self.assertEqual(config["asr_language"], "fr")
        self.assertEqual(config["mfa_dictionary"], "french_mfa")
        self.assertEqual(config["mfa_acoustic_model"], "french_mfa")

    def test_overrides_still_apply_for_worker_specific_keys(self) -> None:
        config = build_dataset_worker_config(
            language="en",
            whisper_model_size="large-v3",
            overrides={"vad_threshold": 0.6},
        )
        self.assertEqual(config["vad_threshold"], 0.6)
        self.assertEqual(config["faster_whisper_beam_size"], DATASET_PROCESSING_DEFAULTS["faster_whisper_beam_size"])

    def test_resolve_whisper_model_size(self) -> None:
        self.assertEqual(resolve_whisper_model("large-v3"), "large-v3")
        self.assertEqual(resolve_whisper_model("base"), "base")

    def test_resolve_asr_device_and_compute_type(self) -> None:
        device, compute_type = resolve_asr_device_and_compute_type()
        self.assertIn(device, {"cuda", "cpu"})
        self.assertIn(compute_type, {"float16", "int8", "float32"})


if __name__ == "__main__":
    unittest.main()
