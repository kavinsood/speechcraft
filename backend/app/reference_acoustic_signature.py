from __future__ import annotations

import math
from typing import Any

import numpy as np


ACOUSTIC_SIGNATURE_V2_DIMENSION = 25
ACOUSTIC_SIGNATURE_V2_ID = "acoustic_signature_v2:normalized_mono_pcm:v2"
ACOUSTIC_SIGNATURE_V2_NAME = "acoustic_signature_v2"


def mono_pcm16_samples_from_wav_bytes(audio_bytes: bytes) -> tuple[np.ndarray, int]:
    import wave
    from io import BytesIO

    with wave.open(BytesIO(audio_bytes), "rb") as handle:
        channels = handle.getnchannels()
        sample_width = handle.getsampwidth()
        sample_rate = handle.getframerate()
        frame_count = handle.getnframes()
        raw = handle.readframes(frame_count)

    if sample_width != 2:
        raise ValueError("acoustic_signature_v2 expects 16-bit PCM WAV input")
    if frame_count <= 0:
        return np.zeros(0, dtype=np.float32), sample_rate

    samples = np.frombuffer(raw, dtype="<i2").astype(np.float32) / 32767.0
    if channels == 1:
        return samples, sample_rate
    if channels == 2:
        left = samples[0::2]
        right = samples[1::2]
        return (left + right) * 0.5, sample_rate
    reshaped = samples.reshape(-1, channels)
    return reshaped.mean(axis=1), sample_rate


def crop_mono_samples(samples: np.ndarray, sample_rate: int, start_seconds: float, end_seconds: float) -> np.ndarray:
    if samples.size == 0:
        return samples
    start_index = max(int(round(start_seconds * sample_rate)), 0)
    end_index = min(int(round(end_seconds * sample_rate)), len(samples))
    if end_index <= start_index:
        return np.zeros(0, dtype=np.float32)
    return samples[start_index:end_index]


def _estimate_pitch_mean_std(samples: np.ndarray, sample_rate: int) -> tuple[float, float]:
    if samples.size < sample_rate // 20:
        return 0.0, 0.0
    stride = max(samples.size // 4000, 1)
    reduced = samples[::stride]
    if reduced.size < 32:
        return 0.0, 0.0

    pitches: list[float] = []
    window = max(int(sample_rate / stride / 100), 32)
    hop = max(window // 2, 16)
    for start in range(0, len(reduced) - window, hop):
        segment = reduced[start : start + window]
        if float(np.max(np.abs(segment))) < 0.01:
            continue
        autocorr = np.correlate(segment, segment, mode="full")[len(segment) - 1 :]
        if autocorr.size < 3:
            continue
        autocorr[0] = 0.0
        peak = int(np.argmax(autocorr[1:])) + 1
        if peak <= 0:
            continue
        pitch_hz = (sample_rate / stride) / peak
        if 60.0 <= pitch_hz <= 500.0:
            pitches.append(pitch_hz)
    if not pitches:
        return 0.0, 0.0
    pitch_values = np.asarray(pitches, dtype=np.float32)
    return float(pitch_values.mean()), float(pitch_values.std())


def _spectral_centroid_stats(samples: np.ndarray, sample_rate: int) -> tuple[float, float]:
    if samples.size < 16:
        return 0.0, 0.0
    stride = max(samples.size // 4000, 1)
    reduced = samples[::stride]
    spectrum = np.abs(np.fft.rfft(reduced * np.hanning(len(reduced))))
    freqs = np.fft.rfftfreq(len(reduced), d=1.0 / (sample_rate / stride))
    total = float(spectrum.sum())
    if total <= 1e-12:
        return 0.0, 0.0
    centroid = float((spectrum * freqs).sum() / total)
    spread = float(math.sqrt(((spectrum * (freqs - centroid) ** 2).sum()) / total))
    return centroid, spread


def _speaking_rate_proxy(samples: np.ndarray, sample_rate: int) -> float:
    if samples.size < sample_rate // 10:
        return 0.0
    frame = max(sample_rate // 50, 1)
    hop = max(frame // 2, 1)
    energies: list[float] = []
    for start in range(0, len(samples) - frame, hop):
        segment = samples[start : start + frame]
        energies.append(float(np.sqrt(np.mean(segment * segment))))
    if len(energies) < 3:
        return 0.0
    threshold = max(float(np.median(energies)) * 1.35, 0.01)
    peaks = 0
    for previous, current, nxt in zip(energies, energies[1:], energies[2:]):
        if current >= threshold and current >= previous and current >= nxt:
            peaks += 1
    duration = len(samples) / sample_rate
    return peaks / max(duration, 0.001)


def acoustic_signature_v2_features(samples: np.ndarray, sample_rate: int) -> list[float]:
    if samples.size == 0 or sample_rate <= 0:
        return [0.0] * ACOUSTIC_SIGNATURE_V2_DIMENSION

    abs_samples = np.abs(samples)
    rms = float(np.sqrt(np.mean(samples * samples)))
    mean_abs = float(np.mean(abs_samples))
    deltas = np.diff(samples) if samples.size > 1 else np.zeros(1, dtype=np.float32)
    mean_delta = float(np.mean(np.abs(deltas))) if deltas.size else 0.0
    zero_crossings = int(np.sum((samples[:-1] < 0) & (samples[1:] >= 0)) + np.sum((samples[:-1] > 0) & (samples[1:] <= 0)))
    zcr = zero_crossings / max(samples.size - 1, 1)
    peak = float(np.max(abs_samples))
    pitch_mean, pitch_std = _estimate_pitch_mean_std(samples, sample_rate)
    centroid_mean, centroid_spread = _spectral_centroid_stats(samples, sample_rate)
    speaking_rate = _speaking_rate_proxy(samples, sample_rate)

    segment_features: list[float] = []
    segment_count = 6
    length = samples.size
    for segment_index in range(segment_count):
        start = int(segment_index * length / segment_count)
        end = int((segment_index + 1) * length / segment_count)
        segment = samples[start:end]
        if segment.size == 0:
            segment_features.extend([0.0, 0.0, 0.0])
            continue
        segment_rms = float(np.sqrt(np.mean(segment * segment)))
        segment_zcr = float(
            np.sum((segment[:-1] < 0) & (segment[1:] >= 0)) + np.sum((segment[:-1] > 0) & (segment[1:] <= 0))
        ) / max(segment.size - 1, 1)
        segment_pitch, _ = _estimate_pitch_mean_std(segment, sample_rate)
        segment_features.extend([segment_rms, segment_zcr, segment_pitch])

    return [
        rms,
        mean_abs,
        mean_delta,
        zcr,
        peak,
        pitch_mean,
        pitch_std,
        centroid_mean,
        centroid_spread,
        speaking_rate,
    ] + segment_features


def acoustic_signature_v2_from_samples(samples: np.ndarray, sample_rate: int) -> list[float]:
    return normalize_embedding_vector(acoustic_signature_v2_features(samples, sample_rate))


def normalize_embedding_vector(vector: list[float]) -> list[float]:
    norm = math.sqrt(sum(value * value for value in vector))
    if norm <= 1e-12:
        return [0.0 for _ in vector]
    return [float(value / norm) for value in vector]


def zscore_rows(embeddings: list[list[float]]) -> list[list[float]]:
    if not embeddings:
        return []
    matrix = np.asarray(embeddings, dtype=np.float64)
    mean = matrix.mean(axis=0)
    std = matrix.std(axis=0)
    std = np.where(std < 1e-12, 1.0, std)
    normalized = (matrix - mean) / std
    return normalized.tolist()


def cosine_similarity(first: list[float], second: list[float]) -> float:
    if len(first) != len(second):
        raise ValueError("Embedding dimensions do not match")
    return float(sum(left * right for left, right in zip(first, second)))


def nms_candidate_indices(
    starts: list[float],
    ends: list[float],
    scores: list[float],
    *,
    overlap_threshold: float,
) -> list[int]:
    order = sorted(range(len(starts)), key=lambda index: scores[index], reverse=True)
    kept: list[int] = []
    for index in order:
        suppressed = False
        for existing in kept:
            overlap = min(ends[index], ends[existing]) - max(starts[index], starts[existing])
            if overlap <= 0:
                continue
            shorter = min(ends[index] - starts[index], ends[existing] - starts[existing])
            if shorter <= 0:
                continue
            if overlap / shorter >= overlap_threshold:
                suppressed = True
                break
        if not suppressed:
            kept.append(index)
    return kept


HDBSCAN_DEFAULT_MIN_CLUSTER_SIZE = 10


def hdbscan_min_cluster_size_for_count(candidate_count: int) -> int:
    return max(5, int(candidate_count * 0.03))


def hdbscan_cluster_labels(
    normalized_embeddings: list[list[float]],
    *,
    min_cluster_size: int = HDBSCAN_DEFAULT_MIN_CLUSTER_SIZE,
) -> list[int]:
    if not normalized_embeddings:
        return []
    matrix = np.asarray(normalized_embeddings, dtype=np.float64)
    if matrix.shape[0] == 1:
        return [0]
    from sklearn.cluster import HDBSCAN

    effective_min_cluster_size = min(max(int(min_cluster_size), 2), matrix.shape[0])
    clusterer = HDBSCAN(metric="euclidean", min_cluster_size=effective_min_cluster_size)
    return clusterer.fit_predict(matrix).astype(int).tolist()


def dumb_cluster_display_labels(cluster_labels: list[int]) -> dict[int, str]:
    labels: dict[int, str] = {}
    if -1 in cluster_labels:
        labels[-1] = "outliers"
    dense_cluster_ids = sorted({cluster_id for cluster_id in cluster_labels if cluster_id >= 0})
    for index, cluster_id in enumerate(dense_cluster_ids, start=1):
        labels[cluster_id] = str(index)
    return labels


def cluster_risk_flag_for_label(display_label: str) -> str:
    if display_label == "outliers":
        return "cluster_outliers"
    return f"cluster_{display_label}"


def embedding_space_descriptor_v2(dimension: int) -> dict[str, Any]:
    return {
        "id": ACOUSTIC_SIGNATURE_V2_ID,
        "name": ACOUSTIC_SIGNATURE_V2_NAME,
        "family": "acoustic_signature",
        "version": 2,
        "domain": "deterministic_acoustic_feature_vector",
        "normalized": True,
        "source_format": "normalized_mono_pcm",
        "preprocessing": {
            "channel_render_policy": "mono_average",
            "downsample_policy": "analysis_stride_cap_4000",
            "windowing_policy": "global_plus_six_segments",
        },
        "features": [
            "global_rms",
            "global_mean_abs",
            "global_mean_delta",
            "global_zero_crossing_rate",
            "global_peak",
            "pitch_mean",
            "pitch_std",
            "spectral_centroid_mean",
            "spectral_centroid_spread",
            "speaking_rate_proxy",
            "segment_rms_zcr_pitch_x6",
        ],
        "dimension": dimension,
    }
