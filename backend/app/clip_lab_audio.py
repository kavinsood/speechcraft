"""Dataset Clip Lab PCM16 mono audio revision renderer (v1)."""

from __future__ import annotations

import hashlib
import io
import json
import os
import wave
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import numpy as np

from .clip_lab_state import CLIP_LAB_PEAKS_REL, CLIP_LAB_RENDERS_REL

RENDERER_VERSION = "1"
AUDIO_REVISION_PREFIX = "speechcraft-clip-lab-audio-v1"
PEAK_BIN_COUNT = 960
MAX_INSERT_SILENCE_SEC = 5.0
MAX_EXTRA_DURATION_SEC = 10.0

DELETE_RANGE_KEYS = frozenset({"kind", "start_sample", "end_sample"})
INSERT_SILENCE_KEYS = frozenset({"kind", "at_sample", "duration_samples"})

AudioOpKind = Literal["delete_range", "insert_silence"]


class ClipLabAudioValidationError(ValueError):
    """Invalid audio edit operation or PCM contract violation."""


class ClipLabSourceIdentityError(ClipLabAudioValidationError):
    """Candidate WAV bytes do not match manifest source identity."""


def canonical_recipe_json(ops: list[dict[str, Any]]) -> str:
    payload = {"schema_version": 1, "ops": ops}
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _timeline_length_after_op(timeline_length: int, op: dict[str, Any]) -> int:
    kind = op["kind"]
    if kind == "delete_range":
        return timeline_length - (int(op["end_sample"]) - int(op["start_sample"]))
    if kind == "insert_silence":
        return timeline_length + int(op["duration_samples"])
    raise ClipLabAudioValidationError(f"unsupported kind {kind!r}")


def validate_audio_ops_recipe(
    ops: list[dict[str, Any]],
    *,
    source_sample_count: int,
    sample_rate: int,
) -> None:
    if source_sample_count < 0:
        raise ClipLabAudioValidationError("source_sample_count must be >= 0")
    if sample_rate <= 0:
        raise ClipLabAudioValidationError("sample_rate must be > 0")
    current = source_sample_count
    max_len = source_sample_count + int(sample_rate * MAX_EXTRA_DURATION_SEC)
    for index, op in enumerate(ops):
        validate_audio_op(op, current, index=index, sample_rate=sample_rate)
        current = _timeline_length_after_op(current, op)
        if current == 0:
            raise ClipLabAudioValidationError("audio edit cannot remove the entire clip")
        if current > max_len:
            raise ClipLabAudioValidationError("audio edit exceeds maximum edited duration")


def compute_audio_revision_hash(
    source_audio_sha256: str,
    ops: list[dict[str, Any]],
    *,
    renderer_version: str = RENDERER_VERSION,
    source_sample_count: int | None = None,
    sample_rate: int | None = None,
) -> str | None:
    if not ops:
        return None
    if source_sample_count is not None and sample_rate is not None:
        validate_audio_ops_recipe(ops, source_sample_count=source_sample_count, sample_rate=sample_rate)
    recipe = canonical_recipe_json(ops)
    payload = f"{AUDIO_REVISION_PREFIX}\n{source_audio_sha256}\n{renderer_version}\n{recipe}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_source_wav_bytes(payload: bytes, expected_sha256: str) -> None:
    if expected_sha256.startswith("sha256:"):
        expected_sha256 = expected_sha256[7:]
    actual_sha256 = hashlib.sha256(payload).hexdigest()
    if actual_sha256 != expected_sha256:
        raise ClipLabSourceIdentityError(
            f"source WAV hash mismatch: expected {expected_sha256}, got {actual_sha256}"
        )


def verify_source_wav_identity(path: Path, expected_sha256: str) -> None:
    if not path.is_file():
        raise ClipLabSourceIdentityError(f"source WAV is missing: {path}")
    verify_source_wav_bytes(path.read_bytes(), expected_sha256)


def _read_pcm16_mono_wav_handle(handle: wave.Wave_read) -> tuple[np.ndarray, int]:
    channels = handle.getnchannels()
    sample_width = handle.getsampwidth()
    sample_rate = handle.getframerate()
    compression = handle.getcomptype()
    if channels != 1:
        raise ClipLabAudioValidationError(f"expected mono WAV, got {channels} channels")
    if sample_width != 2:
        raise ClipLabAudioValidationError(f"expected PCM16 WAV, got sample width {sample_width}")
    if compression != "NONE":
        raise ClipLabAudioValidationError(f"expected uncompressed PCM WAV, got compression {compression!r}")
    if sample_rate <= 0:
        raise ClipLabAudioValidationError(f"expected positive sample rate, got {sample_rate}")
    frames = handle.readframes(handle.getnframes())
    if not frames:
        return np.array([], dtype=np.int16), int(sample_rate)
    samples = np.frombuffer(frames, dtype="<i2").copy()
    return samples, int(sample_rate)


def load_pcm16_mono_wav_bytes(payload: bytes) -> tuple[np.ndarray, int]:
    with wave.open(io.BytesIO(payload), "rb") as handle:
        return _read_pcm16_mono_wav_handle(handle)


def load_pcm16_mono_wav(path: Path) -> tuple[np.ndarray, int]:
    with wave.open(str(path), "rb") as handle:
        return _read_pcm16_mono_wav_handle(handle)


def write_pcm16_mono_wav(path: Path, samples: np.ndarray, sample_rate: int) -> None:
    if samples.ndim != 1:
        raise ClipLabAudioValidationError(f"expected 1-D sample array, got shape {samples.shape!r}")
    if sample_rate <= 0:
        raise ClipLabAudioValidationError(f"sample_rate must be > 0, got {sample_rate}")
    path.parent.mkdir(parents=True, exist_ok=True)
    clipped = np.asarray(samples, dtype=np.int16)
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(int(sample_rate))
        handle.writeframes(clipped.tobytes())


def _reject_unknown_keys(op: dict[str, Any], allowed_keys: frozenset[str], *, index: int) -> None:
    unexpected = set(op) - allowed_keys
    if unexpected:
        joined = ", ".join(sorted(unexpected))
        raise ClipLabAudioValidationError(f"op {index}: unexpected field(s): {joined}")


def _validate_delete_range(op: dict[str, Any], timeline_length: int, *, index: int) -> None:
    _reject_unknown_keys(op, DELETE_RANGE_KEYS, index=index)
    if "start_sample" not in op or "end_sample" not in op:
        raise ClipLabAudioValidationError(f"op {index}: delete_range requires start_sample and end_sample")
    start = op["start_sample"]
    end = op["end_sample"]
    if isinstance(start, bool) or isinstance(end, bool) or not isinstance(start, int) or not isinstance(end, int):
        raise ClipLabAudioValidationError(f"op {index}: delete_range sample coordinates must be integers")
    if start < 0 or end <= start or end > timeline_length:
        raise ClipLabAudioValidationError(
            f"op {index}: delete_range [{start}, {end}) is invalid for timeline length {timeline_length}"
        )


def _validate_insert_silence(
    op: dict[str, Any],
    timeline_length: int,
    *,
    index: int,
    sample_rate: int,
) -> None:
    _reject_unknown_keys(op, INSERT_SILENCE_KEYS, index=index)
    if "at_sample" not in op or "duration_samples" not in op:
        raise ClipLabAudioValidationError(f"op {index}: insert_silence requires at_sample and duration_samples")
    at_sample = op["at_sample"]
    duration_samples = op["duration_samples"]
    if (
        isinstance(at_sample, bool)
        or isinstance(duration_samples, bool)
        or not isinstance(at_sample, int)
        or not isinstance(duration_samples, int)
    ):
        raise ClipLabAudioValidationError(f"op {index}: insert_silence coordinates must be integers")
    if at_sample < 0 or at_sample > timeline_length or duration_samples <= 0:
        raise ClipLabAudioValidationError(
            f"op {index}: insert_silence at={at_sample} duration={duration_samples} invalid for length {timeline_length}"
        )
    max_insert_samples = int(sample_rate * MAX_INSERT_SILENCE_SEC)
    if duration_samples > max_insert_samples:
        raise ClipLabAudioValidationError(
            f"op {index}: insert_silence duration exceeds {MAX_INSERT_SILENCE_SEC:g} seconds"
        )


def validate_audio_op(
    op: dict[str, Any],
    timeline_length: int,
    *,
    index: int,
    sample_rate: int,
) -> AudioOpKind:
    kind = op.get("kind")
    if kind == "delete_range":
        _validate_delete_range(op, timeline_length, index=index)
        return kind
    if kind == "insert_silence":
        _validate_insert_silence(op, timeline_length, index=index, sample_rate=sample_rate)
        return kind
    raise ClipLabAudioValidationError(f"op {index}: unsupported kind {kind!r}")


def apply_audio_op(samples: np.ndarray, op: dict[str, Any]) -> np.ndarray:
    kind = op["kind"]
    if kind == "delete_range":
        start = int(op["start_sample"])
        end = int(op["end_sample"])
        return np.concatenate([samples[:start], samples[end:]])
    if kind == "insert_silence":
        at_sample = int(op["at_sample"])
        duration_samples = int(op["duration_samples"])
        silence = np.zeros(duration_samples, dtype=np.int16)
        return np.concatenate([samples[:at_sample], silence, samples[at_sample:]])
    raise ClipLabAudioValidationError(f"unsupported kind {kind!r}")


def apply_audio_ops(samples: np.ndarray, ops: list[dict[str, Any]], *, sample_rate: int) -> np.ndarray:
    if sample_rate <= 0:
        raise ClipLabAudioValidationError("sample_rate must be > 0")
    initial_len = len(samples)
    max_len = initial_len + int(sample_rate * MAX_EXTRA_DURATION_SEC)
    current = np.asarray(samples, dtype=np.int16)
    for index, op in enumerate(ops):
        validate_audio_op(op, len(current), index=index, sample_rate=sample_rate)
        current = apply_audio_op(current, op)
        if len(current) == 0:
            raise ClipLabAudioValidationError("audio edit cannot remove the entire clip")
        if len(current) > max_len:
            raise ClipLabAudioValidationError("audio edit exceeds maximum edited duration")
    return current


def compute_waveform_peaks(samples: np.ndarray) -> list[float]:
    if len(samples) == 0:
        return [0.0] * PEAK_BIN_COUNT
    normalized = np.abs(samples.astype(np.float64) / 32767.0)
    chunks = np.array_split(normalized, PEAK_BIN_COUNT)
    return [min(1.0, float(chunk.max())) if len(chunk) > 0 else 0.0 for chunk in chunks]


def render_cache_path(run_root: Path, clip_id: str, audio_revision_hash: str) -> Path:
    return run_root / CLIP_LAB_RENDERS_REL / clip_id / f"{audio_revision_hash}.wav"


def peaks_cache_path(run_root: Path, effective_revision_key: str) -> Path:
    return run_root / CLIP_LAB_PEAKS_REL / f"{effective_revision_key}.json"


def fsync_path(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _unique_temp_path(target: Path) -> Path:
    return target.with_name(f"{target.name}.{uuid4().hex}.tmp")


def atomic_publish_bytes(target: Path, payload: bytes) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.is_file():
        return
    tmp_path = _unique_temp_path(target)
    try:
        tmp_path.write_bytes(payload)
        fsync_path(tmp_path)
        if target.is_file():
            return
        tmp_path.replace(target)
    finally:
        if tmp_path.exists() and not target.is_file():
            tmp_path.unlink(missing_ok=True)
        elif tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def atomic_publish_file(source: Path, target: Path) -> None:
    atomic_publish_bytes(target, source.read_bytes())


def render_audio_ops_to_wav_from_bytes(
    *,
    source_wav_bytes: bytes,
    ops: list[dict[str, Any]],
    output_path: Path,
    source_audio_sha256: str | None = None,
) -> tuple[np.ndarray, int]:
    if source_audio_sha256 is not None:
        verify_source_wav_bytes(source_wav_bytes, source_audio_sha256)
    samples, sample_rate = load_pcm16_mono_wav_bytes(source_wav_bytes)
    rendered = apply_audio_ops(samples, ops, sample_rate=sample_rate)
    write_pcm16_mono_wav(output_path, rendered, sample_rate)
    return rendered, sample_rate


def render_audio_ops_to_wav(
    *,
    source_wav_path: Path,
    ops: list[dict[str, Any]],
    output_path: Path,
    source_audio_sha256: str | None = None,
) -> tuple[np.ndarray, int]:
    if source_audio_sha256 is not None:
        verify_source_wav_identity(source_wav_path, source_audio_sha256)
    samples, sample_rate = load_pcm16_mono_wav(source_wav_path)
    rendered = apply_audio_ops(samples, ops, sample_rate=sample_rate)
    write_pcm16_mono_wav(output_path, rendered, sample_rate)
    return rendered, sample_rate


def render_audio_ops_to_cache_from_bytes(
    *,
    source_wav_bytes: bytes,
    ops: list[dict[str, Any]],
    cache_path: Path,
    source_audio_sha256: str | None = None,
) -> tuple[np.ndarray, int, str]:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if cache_path.is_file():
        samples, sample_rate = load_pcm16_mono_wav(cache_path)
        return samples, sample_rate, sha256_file(cache_path)

    tmp_path = _unique_temp_path(cache_path)
    try:
        rendered, sample_rate = render_audio_ops_to_wav_from_bytes(
            source_wav_bytes=source_wav_bytes,
            ops=ops,
            output_path=tmp_path,
            source_audio_sha256=source_audio_sha256,
        )
        fsync_path(tmp_path)
        rendered_audio_sha256 = sha256_file(tmp_path)
        if cache_path.is_file():
            tmp_path.unlink(missing_ok=True)
            samples, sample_rate = load_pcm16_mono_wav(cache_path)
            return samples, sample_rate, sha256_file(cache_path)
        tmp_path.replace(cache_path)
        return rendered, sample_rate, rendered_audio_sha256
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        if cache_path.is_file():
            samples, sample_rate = load_pcm16_mono_wav(cache_path)
            return samples, sample_rate, sha256_file(cache_path)
        raise


def render_audio_ops_to_cache(
    *,
    source_wav_path: Path,
    ops: list[dict[str, Any]],
    cache_path: Path,
    source_audio_sha256: str | None = None,
) -> tuple[np.ndarray, int, str]:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if cache_path.is_file():
        samples, sample_rate = load_pcm16_mono_wav(cache_path)
        return samples, sample_rate, sha256_file(cache_path)

    tmp_path = _unique_temp_path(cache_path)
    try:
        rendered, sample_rate = render_audio_ops_to_wav(
            source_wav_path=source_wav_path,
            ops=ops,
            output_path=tmp_path,
            source_audio_sha256=source_audio_sha256,
        )
        fsync_path(tmp_path)
        rendered_audio_sha256 = sha256_file(tmp_path)
        if cache_path.is_file():
            tmp_path.unlink(missing_ok=True)
            samples, sample_rate = load_pcm16_mono_wav(cache_path)
            return samples, sample_rate, sha256_file(cache_path)
        tmp_path.replace(cache_path)
        return rendered, sample_rate, rendered_audio_sha256
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        if cache_path.is_file():
            samples, sample_rate = load_pcm16_mono_wav(cache_path)
            return samples, sample_rate, sha256_file(cache_path)
        raise


def render_or_reuse_audio_revision_from_bytes(
    *,
    source_wav_bytes: bytes,
    ops: list[dict[str, Any]],
    cache_path: Path,
    peaks_path: Path,
    revision_key: str,
    source_audio_sha256: str,
) -> tuple[np.ndarray, int, str]:
    samples, sample_rate, rendered_audio_sha256 = render_audio_ops_to_cache_from_bytes(
        source_wav_bytes=source_wav_bytes,
        ops=ops,
        cache_path=cache_path,
        source_audio_sha256=source_audio_sha256,
    )
    if not peaks_path.is_file():
        payload = build_peaks_payload(
            revision_key=revision_key,
            samples=samples,
            sample_rate=sample_rate,
        )
        atomic_publish_peaks_payload(peaks_path, payload)
    return samples, sample_rate, rendered_audio_sha256


def render_or_reuse_audio_revision(
    *,
    source_wav_path: Path,
    ops: list[dict[str, Any]],
    cache_path: Path,
    peaks_path: Path,
    revision_key: str,
    source_audio_sha256: str,
) -> tuple[np.ndarray, int, str]:
    samples, sample_rate, rendered_audio_sha256 = render_audio_ops_to_cache(
        source_wav_path=source_wav_path,
        ops=ops,
        cache_path=cache_path,
        source_audio_sha256=source_audio_sha256,
    )
    if not peaks_path.is_file():
        payload = build_peaks_payload(
            revision_key=revision_key,
            samples=samples,
            sample_rate=sample_rate,
        )
        atomic_publish_peaks_payload(peaks_path, payload)
    return samples, sample_rate, rendered_audio_sha256


def atomic_publish_peaks_payload(target: Path, payload: dict[str, Any]) -> None:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    atomic_publish_bytes(target, encoded)


def build_peaks_payload(
    *,
    revision_key: str,
    samples: np.ndarray,
    sample_rate: int,
) -> dict[str, Any]:
    duration_sec = round(len(samples) / max(sample_rate, 1), 6)
    return {
        "revision_key": revision_key,
        "bins": PEAK_BIN_COUNT,
        "peaks": compute_waveform_peaks(samples),
        "duration_sec": duration_sec,
        "sample_rate_hz": sample_rate,
    }
