from __future__ import annotations

from typing import Any, Literal

WhisperModelSize = Literal["large-v3", "base"]

LANGUAGE_OPTIONS: dict[str, str] = {
    "auto": "Auto-detect (Whisper)",
    "en": "English",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "it": "Italian",
    "pt": "Portuguese",
}

MFA_MODELS_BY_LANGUAGE: dict[str, tuple[str, str]] = {
    "en": ("english_us_mfa", "english_mfa"),
    "es": ("spanish_mfa", "spanish_mfa"),
    "fr": ("french_mfa", "french_mfa"),
    "de": ("german_mfa", "german_mfa"),
    "it": ("italian_mfa", "italian_mfa"),
    "pt": ("portuguese_brazil_mfa", "portuguese_brazil_mfa"),
}

WHISPER_MODEL_BY_SIZE: dict[str, str] = {
    "large-v3": "large-v3",
    "base": "base",
}

DATASET_PROCESSING_DEFAULTS: dict[str, Any] = {
    "max_processing_buffer_sec": 29.5,
    "processing_buffer_pad_sec": 0.25,
    "target_processing_chunk_sec": 25.0,
    "min_split_gap_sec": 0.15,
    "min_asr_mfa_buffer_sec": 1.0,
    "faster_whisper_beam_size": 5,
    "asr_model_load_timeout_sec": 60,
    "asr_transcribe_timeout_sec": 300,
    "mfa_timeout_sec": 600,
    "alignment_tiny_word_sec": 0.02,
    "alignment_long_word_sec": 2.0,
    "alignment_trusted_edge_warn_sec": 0.08,
    "asr_task": "transcribe",
    "asr_vad_filter": False,
    "asr_condition_on_previous_text": False,
    "asr_word_timestamps": False,
    "mfa_single_speaker": True,
}

DATASET_SLICER_HARDCODED: dict[str, Any] = {
    "cutpoint_frame_ms": 10,
    "cutpoint_hop_ms": 5,
    "cutpoint_noise_margin_db": 6.0,
    "oov_cut_guard_sec": 0.5,
    "symbol_cut_guard_sec": 0.5,
    "numeric_cut_guard_sec": 0.5,
    "provisional_split_guard_sec": 0.5,
}

DATASET_SLICER_DEFAULTS: dict[str, Any] = {
    **DATASET_SLICER_HARDCODED,
    "cutpoint_left_word_edge_guard_ms": 30,
    "cutpoint_min_gap_ms": 80,
    "cutpoint_right_word_edge_guard_ms": 30,
    "candidate_min_clip_sec": 3.0,
    "candidate_target_clip_sec": 8.0,
    "candidate_max_clip_sec": 15.0,
}

SLICER_UI_CONFIG_KEYS = frozenset(
    {
        "candidate_min_clip_sec",
        "candidate_target_clip_sec",
        "candidate_max_clip_sec",
        "cutpoint_min_gap_ms",
        "cutpoint_left_word_edge_guard_ms",
        "cutpoint_right_word_edge_guard_ms",
    }
)


def resolve_asr_device_and_compute_type() -> tuple[str, str]:
    try:
        import torch
    except ImportError:
        return "cpu", "int8"

    if torch.cuda.is_available():
        return "cuda", "float16"
    return "cpu", "int8"


def resolve_mfa_models(language: str) -> tuple[str, str]:
    normalized = (language or "en").strip().lower()
    if normalized in {"", "auto"}:
        normalized = "en"
    return MFA_MODELS_BY_LANGUAGE.get(normalized, MFA_MODELS_BY_LANGUAGE["en"])


def resolve_whisper_model(model_size: str) -> str:
    return WHISPER_MODEL_BY_SIZE.get(model_size, WHISPER_MODEL_BY_SIZE["large-v3"])


def resolve_asr_language(language: str) -> str | None:
    normalized = (language or "auto").strip().lower()
    if normalized in {"", "auto"}:
        return None
    return normalized


def build_slicer_config_overrides(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    config = dict(DATASET_SLICER_DEFAULTS)
    if overrides:
        for key, value in overrides.items():
            if key in SLICER_UI_CONFIG_KEYS:
                config[key] = value
    return config


def build_dataset_worker_config(
    *,
    language: str = "auto",
    whisper_model_size: WhisperModelSize = "large-v3",
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    mfa_dictionary, mfa_acoustic_model = resolve_mfa_models(language)
    asr_device, asr_compute_type = resolve_asr_device_and_compute_type()
    asr_language = resolve_asr_language(language)

    config: dict[str, Any] = {
        **DATASET_PROCESSING_DEFAULTS,
        **DATASET_SLICER_DEFAULTS,
        "faster_whisper_model": resolve_whisper_model(whisper_model_size),
        "faster_whisper_device": asr_device,
        "faster_whisper_compute_type": asr_compute_type,
        "mfa_dictionary": mfa_dictionary,
        "mfa_acoustic_model": mfa_acoustic_model,
    }
    if asr_language is not None:
        config["asr_language"] = asr_language
    else:
        config["asr_language"] = "auto"

    if overrides:
        config.update(overrides)
    return config
