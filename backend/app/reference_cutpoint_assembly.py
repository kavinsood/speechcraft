from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CutpointWindow:
    source_audio_id: str
    buffer_id: str
    analysis_start_sample: int
    analysis_end_sample: int
    start_cutpoint_id: str
    end_cutpoint_id: str
    transcript_text: str | None


@dataclass(frozen=True)
class SourceAudioMap:
    source_audio_id: str
    recording_id: str
    source_path: Path
    analysis_sample_rate: int
    source_sample_rate: int


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        cleaned = line.strip()
        if cleaned:
            rows.append(json.loads(cleaned))
    return rows


def analysis_to_native_seconds(analysis_sample: int, variant: dict[str, Any]) -> float:
    analysis_rate = int(variant["analysis_sample_rate"])
    source_rate = int(variant["source_sample_rate"])
    native_sample = int(round(analysis_sample * (source_rate / analysis_rate)))
    return native_sample / source_rate


def build_source_audio_maps(
    run_root: Path,
    recording_id_by_path: dict[str, str],
) -> dict[str, SourceAudioMap]:
    source_manifest = read_json(run_root / "artifacts" / "source_audio_manifest.json")
    variant_manifest = read_json(run_root / "artifacts" / "audio_variants_manifest.json")
    variants = {
        str(row["source_audio_id"]): row
        for row in variant_manifest.get("variants", [])
        if str(row.get("kind")) == "analysis_audio"
    }
    maps: dict[str, SourceAudioMap] = {}
    for source in source_manifest.get("sources", []):
        source_audio_id = str(source["source_audio_id"])
        variant = variants.get(source_audio_id)
        if variant is None:
            continue
        source_path = Path(str(source["path"])).resolve()
        recording_id = recording_id_by_path.get(str(source_path))
        if recording_id is None:
            continue
        maps[source_audio_id] = SourceAudioMap(
            source_audio_id=source_audio_id,
            recording_id=recording_id,
            source_path=source_path,
            analysis_sample_rate=int(variant["analysis_sample_rate"]),
            source_sample_rate=int(variant["source_sample_rate"]),
        )
    return maps


def reconstruct_transcript(words: list[dict[str, Any]]) -> str:
    raw_tokens: list[str] = []
    previous_raw_token_id = None
    for word in words:
        raw_token_id = word.get("raw_token_id")
        if raw_token_id and raw_token_id == previous_raw_token_id:
            continue
        raw_tokens.append(str(word.get("raw_token") or word["word"]))
        previous_raw_token_id = raw_token_id
    return " ".join(raw_tokens).strip()


def generate_overlapping_cutpoint_windows(
    cutpoints: list[dict[str, Any]],
    *,
    sample_rate: int,
    min_sec: float,
    max_sec: float,
    target_sec: float,
    stride_cutpoints: int,
) -> list[tuple[int, int, str, str]]:
    if len(cutpoints) < 2:
        return []
    min_samples = max(int(round(min_sec * sample_rate)), 1)
    max_samples = max(int(round(max_sec * sample_rate)), min_samples)
    target_samples = int(round(target_sec * sample_rate))
    ordered = sorted(cutpoints, key=lambda row: int(row["cut_local_sample"]))
    windows: list[tuple[int, int, str, str]] = []
    start_index = 0
    stride = max(int(stride_cutpoints), 1)
    while start_index < len(ordered) - 1:
        start = ordered[start_index]
        start_local = int(start["cut_local_sample"])
        candidates = [
            end
            for end in ordered[start_index + 1 :]
            if min_samples <= int(end["cut_local_sample"]) - start_local <= max_samples
        ]
        if candidates:
            end = min(
                candidates,
                key=lambda row: (
                    abs(int(row["cut_local_sample"]) - start_local - target_samples),
                    int(row["cut_local_sample"]),
                ),
            )
            end_local = int(end["cut_local_sample"])
            if end_local > start_local:
                windows.append((start_local, end_local, str(start["id"]), str(end["id"])))
        start_index += stride
    return windows


def load_cutpoint_windows(
    run_root: Path,
    *,
    target_durations: list[float],
    stride_cutpoints: int,
) -> list[CutpointWindow]:
    cutpoints_path = run_root / "artifacts" / "safe_cutpoints.jsonl"
    if not cutpoints_path.exists():
        return []
    buffers = read_json(run_root / "artifacts" / "processing_buffers.json")
    if not isinstance(buffers, list):
        return []
    words = read_jsonl(run_root / "artifacts" / "aligned_words.jsonl")
    words_by_buffer: dict[str, list[dict[str, Any]]] = {}
    for word in words:
        words_by_buffer.setdefault(str(word["buffer_id"]), []).append(word)
    cutpoints_by_buffer: dict[str, list[dict[str, Any]]] = {}
    for cutpoint in read_jsonl(cutpoints_path):
        cutpoints_by_buffer.setdefault(str(cutpoint["buffer_id"]), []).append(cutpoint)

    min_sec = min(target_durations)
    max_sec = max(max(target_durations) + 2.0, min_sec + 1.0)
    windows: list[CutpointWindow] = []
    for buffer in buffers:
        buffer_id = str(buffer["buffer_id"])
        source_audio_id = str(buffer["source_audio_id"])
        sample_rate = int(buffer.get("sample_rate") or 16000)
        buffer_cutpoints = cutpoints_by_buffer.get(buffer_id, [])
        buffer_words = sorted(
            words_by_buffer.get(buffer_id, []),
            key=lambda row: int(row["source_start_sample"]),
        )
        buffer_base = int(buffer["source_start_sample"])
        for target_sec in target_durations:
            for start_local, end_local, start_id, end_id in generate_overlapping_cutpoint_windows(
                buffer_cutpoints,
                sample_rate=sample_rate,
                min_sec=min_sec,
                max_sec=max_sec,
                target_sec=target_sec,
                stride_cutpoints=stride_cutpoints,
            ):
                analysis_start = buffer_base + start_local
                analysis_end = buffer_base + end_local
                included_words = [
                    word
                    for word in buffer_words
                    if int(word["source_start_sample"]) >= analysis_start
                    and int(word["source_end_sample"]) <= analysis_end
                ]
                windows.append(
                    CutpointWindow(
                        source_audio_id=source_audio_id,
                        buffer_id=buffer_id,
                        analysis_start_sample=analysis_start,
                        analysis_end_sample=analysis_end,
                        start_cutpoint_id=start_id,
                        end_cutpoint_id=end_id,
                        transcript_text=reconstruct_transcript(included_words) or None,
                    )
                )
    return windows
