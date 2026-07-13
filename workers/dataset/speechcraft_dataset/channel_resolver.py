"""Intelligent stereo-channel resolver (compute -> decide | escalate).

For stereo sources whose two channels carry *different* content (e.g. target
speaker on one channel, an interviewer/room on the other), a blind downmix to
mono blends the contaminant into the training audio. Rather than ask the user,
we compute discriminating signals and decide automatically, escalating to a
prompt only when the computation is genuinely inconclusive:

    - inter-channel correlation ~1.0  -> matched stereo, downmix freely
    - low correlation (divergent)      -> compare per-channel speech energy:
        * one channel clearly dominant -> pick it (left/right)
        * energies comparable          -> escalate (prompt the user)

Same resolver pattern as language/model detection: a pure `decide(signals)`
core with an explicit escalation policy. Implemented in the stdlib only (the
prep stage is otherwise pure-ffmpeg and must not require numpy). An optional
speaker-embedding tiebreak can be layered on the ambiguous branch later.
"""

from __future__ import annotations

import math
import wave
from array import array
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Sequence

ChannelDecision = Literal["downmix", "left", "right", "prompt"]

CORRELATION_MATCHED = 0.98
SPEECH_ENERGY_DOMINANT_RATIO = 1.5
FRAME_MS = 20.0
VOICED_REL_THRESHOLD = 0.15
MAX_ANALYSIS_SEC = 120.0  # cap analysis window; channel character is stable


@dataclass
class ChannelResolution:
    decision: ChannelDecision
    correlation: float
    left_energy: float
    right_energy: float
    reason: str
    reason_codes: list[str] = field(default_factory=list)


def _pearson(a: Sequence[int], b: Sequence[int]) -> float:
    n = min(len(a), len(b))
    if n == 0:
        return 0.0
    sa = sb = saa = sbb = sab = 0.0
    for i in range(n):
        x = float(a[i])
        y = float(b[i])
        sa += x
        sb += y
        saa += x * x
        sbb += y * y
        sab += x * y
    var_a = saa - sa * sa / n
    var_b = sbb - sb * sb / n
    if var_a <= 0.0 or var_b <= 0.0:
        # A silent/constant channel is not "matched" content unless identical.
        return 1.0 if (var_a == 0.0 and var_b == 0.0 and list(a[:n]) == list(b[:n])) else 0.0
    cov = sab - sa * sb / n
    denom = math.sqrt(var_a * var_b)
    if denom == 0.0:
        return 0.0
    return max(-1.0, min(1.0, cov / denom))


def _speech_energy(channel: Sequence[int], sample_rate: int) -> float:
    """Mean RMS over voiced frames — a speech-loudness proxy (energy VAD)."""
    n = len(channel)
    if n == 0 or sample_rate <= 0:
        return 0.0
    peak = 0
    for v in channel:
        av = -v if v < 0 else v
        if av > peak:
            peak = av
    if peak == 0:
        return 0.0
    frame = max(1, int(sample_rate * FRAME_MS / 1000.0))
    frame_rms: list[float] = []
    for start in range(0, n - frame + 1, frame):
        acc = 0.0
        for i in range(start, start + frame):
            x = channel[i] / peak
            acc += x * x
        frame_rms.append(math.sqrt(acc / frame))
    if not frame_rms:
        acc = sum((channel[i] / peak) ** 2 for i in range(n))
        return math.sqrt(acc / n)
    max_rms = max(frame_rms)
    gate = VOICED_REL_THRESHOLD * max_rms
    voiced = [r for r in frame_rms if r > gate]
    if not voiced:
        return 0.0
    return sum(voiced) / len(voiced)


def decide_channel(
    left: Sequence[int],
    right: Sequence[int],
    *,
    sample_rate: int,
    correlation_matched: float = CORRELATION_MATCHED,
    dominant_ratio: float = SPEECH_ENERGY_DOMINANT_RATIO,
) -> ChannelResolution:
    """Pure decision core. See module docstring for the policy."""
    cap = int(MAX_ANALYSIS_SEC * sample_rate) if sample_rate > 0 else len(left)
    left = left[:cap]
    right = right[:cap]

    correlation = _pearson(left, right)
    left_energy = _speech_energy(left, sample_rate)
    right_energy = _speech_energy(right, sample_rate)

    if correlation >= correlation_matched:
        return ChannelResolution("downmix", correlation, left_energy, right_energy, "matched_stereo")

    hi = max(left_energy, right_energy)
    lo = min(left_energy, right_energy)
    if hi <= 1e-6:
        return ChannelResolution(
            "downmix", correlation, left_energy, right_energy, "both_channels_silent"
        )

    ratio = hi / lo if lo > 1e-9 else float("inf")
    if ratio >= dominant_ratio:
        winner: ChannelDecision = "left" if left_energy >= right_energy else "right"
        return ChannelResolution(
            winner, correlation, left_energy, right_energy, "dominant_speech_channel"
        )

    return ChannelResolution(
        "prompt",
        correlation,
        left_energy,
        right_energy,
        "ambiguous_divergent_channels",
        reason_codes=["channel_selection_ambiguous"],
    )


def load_stereo_channels(path: Path) -> tuple[array, array, int]:
    """Read a 16-bit WAV as (left, right, sample_rate). Mono -> both == data."""
    with wave.open(str(path), "rb") as handle:
        sample_rate = handle.getframerate()
        n_channels = handle.getnchannels()
        raw = handle.readframes(handle.getnframes())
    data = array("h")
    data.frombytes(raw)
    if n_channels < 2:
        return data, data, sample_rate
    left = data[0::n_channels]
    right = data[1::n_channels]
    return left, right, sample_rate


def resolve_source_channel(path: Path, num_channels: int) -> ChannelResolution | None:
    """Resolve a source WAV's channel decision, or None for non-stereo sources."""
    if num_channels != 2:
        return None
    left, right, sample_rate = load_stereo_channels(path)
    return decide_channel(left, right, sample_rate=sample_rate)


def ffmpeg_channel_args(decision: ChannelDecision) -> list[str]:
    """ffmpeg args to realize a channel decision as a mono analysis stream."""
    if decision == "left":
        return ["-af", "pan=mono|c0=c0"]
    if decision == "right":
        return ["-af", "pan=mono|c0=c1"]
    # downmix + prompt both fall back to the safe average downmix (prompt also
    # records a reason code so the UI can ask; the pipeline still proceeds).
    return ["-ac", "1"]
