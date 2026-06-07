#!/usr/bin/env python3
from __future__ import annotations

import json
import gc
import re
import shutil
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import soundfile as sf
import torch
from silero_vad import get_speech_timestamps, load_silero_vad


SOURCE_AUDIO = Path(
    "/home/aaravthegreat/Projects/Charactors/MadelynCline/raw/"
    "Session 62： Madelyn Cline ｜ Therapuss with Jake Shane [jCUX4DAlKhA].wav"
)
PROJECT_ROOT = Path("/home/aaravthegreat/Projects/speechcraft-tracer-bullet")
OUTPUT_ROOT = Path("/tmp/speechcraft_tracer_output")

SUBSET_DURATION_SEC: float | None = None
SUBSET_BASENAME = "input_subset"

ANALYSIS_SAMPLE_RATE = 16_000
VAD_THRESHOLD = 0.5
MIN_SPEECH_MS = 250
MIN_SILENCE_MS = 250
SPEECH_PAD_MS = 80
NEMO_SPEAKER_MODEL = "titanet_large"
NEMO_MAX_SPEAKERS = 6
NEMO_DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
NEMO_WINDOW_SEC = 900.0
NEMO_WINDOW_OVERLAP_SEC = 30.0
NEMO_BATCH_SIZE = 16
NEMO_SAVE_EMBEDDINGS = False
TARGET_SPEAKER_LABEL = "speaker_0"
FASTER_WHISPER_MODEL = "small.en"
FASTER_WHISPER_DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
FASTER_WHISPER_COMPUTE_TYPE = "float16" if FASTER_WHISPER_DEVICE == "cuda" else "int8"
FASTER_WHISPER_INITIAL_PROMPT = None
TARGET_CHUNK_SEC = 24.0
MAX_PROCESSING_BUFFER_SEC = 29.5
PROCESSING_BUFFER_PAD_SEC = 0.5
SPLIT_SEARCH_WINDOW_SEC = 2.0
MIN_SPLIT_GAP_SEC = 0.12
PROVISIONAL_SPLIT_GUARD_SEC = 0.5
ALIGNMENT_TINY_WORD_SEC = 0.020
ALIGNMENT_LONG_WORD_SEC = 2.0
TRUSTED_EDGE_WARN_SEC = 0.080
CUTPOINT_LEFT_WORD_EDGE_GUARD_SEC = 0.040
CUTPOINT_RIGHT_WORD_EDGE_GUARD_SEC = 0.040
CUTPOINT_MIN_GAP_SEC = 0.120
CUTPOINT_FRAME_SEC = 0.020
CUTPOINT_HOP_SEC = 0.010
CUTPOINT_NOISE_MARGIN_DB = 6.0
OOV_CUT_GUARD_SEC = 0.5
SYMBOL_CUT_GUARD_SEC = 0.5
NUMERIC_CUT_GUARD_SEC = 0.5
MIN_ASR_MFA_BUFFER_SEC = 5.0
MIN_CANDIDATE_CLIP_SEC = 3.0
TARGET_CANDIDATE_CLIP_SEC = 8.0
MAX_CANDIDATE_CLIP_SEC = 15.0
MFA_WORK_ROOT = Path.home() / "Documents" / "MFA"
DANGER_SYMBOLS = set("$%@#*+=&€£¥©™^_")


@dataclass
class VadSegment:
    id: str
    start_sec: float
    end_sec: float
    start_sample: int
    end_sample: int
    backend: str
    backend_version: str
    confidence: float | None = None


@dataclass
class SpeakerRegion:
    id: str
    local_speaker_label: str
    start_sec: float
    end_sec: float
    start_sample: int
    end_sample: int
    backend: str
    backend_version: str | None
    vad_source: str
    rfc_compliant: bool
    source: str


def run(cmd: list[str]) -> str:
    completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return completed.stdout


def ffprobe_json(path: Path) -> dict[str, Any]:
    return json.loads(
        run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration:stream=sample_rate,channels",
                "-of",
                "json",
                str(path),
            ]
        )
    )


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def unique_audio_id(path: Path) -> str:
    return path.stem


def duration_of(path: Path) -> float:
    payload = ffprobe_json(path)
    return float(payload["format"]["duration"])


def subset_window(duration_sec: float, subset_duration_sec: float | None) -> tuple[float, float]:
    if subset_duration_sec is None or subset_duration_sec <= 0:
        return 0.0, duration_sec
    clipped_duration = min(duration_sec, subset_duration_sec)
    start_sec = max(0.0, (duration_sec - clipped_duration) / 2.0)
    end_sec = start_sec + clipped_duration
    return start_sec, end_sec


def extract_subset(source_path: Path, subset_path: Path) -> dict[str, Any]:
    source_duration = duration_of(source_path)
    start_sec, end_sec = subset_window(source_duration, SUBSET_DURATION_SEC)
    run(
        [
            "ffmpeg",
            "-y",
            "-ss",
            f"{start_sec:.3f}",
            "-i",
            str(source_path),
            "-t",
            f"{end_sec - start_sec:.3f}",
            "-acodec",
            "pcm_s16le",
            str(subset_path),
        ]
    )
    return {
        "source_audio": str(source_path),
        "subset_audio": str(subset_path),
        "source_duration_sec": source_duration,
        "subset_start_sec": start_sec,
        "subset_end_sec": end_sec,
        "subset_duration_sec": end_sec - start_sec,
    }


def create_analysis_audio(subset_path: Path, analysis_path: Path) -> dict[str, Any]:
    run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(subset_path),
            "-ac",
            "1",
            "-ar",
            str(ANALYSIS_SAMPLE_RATE),
            "-c:a",
            "pcm_s16le",
            str(analysis_path),
        ]
    )
    payload = ffprobe_json(analysis_path)
    stream = payload["streams"][0]
    return {
        "analysis_audio": str(analysis_path),
        "sample_rate": int(stream["sample_rate"]),
        "channels": int(stream["channels"]),
        "duration_sec": float(payload["format"]["duration"]),
    }


def load_analysis_audio(path: Path) -> tuple[torch.Tensor, int]:
    samples, sample_rate = sf.read(str(path), dtype="float32", always_2d=False)
    if sample_rate != ANALYSIS_SAMPLE_RATE:
        raise ValueError(f"Expected {ANALYSIS_SAMPLE_RATE} Hz analysis audio, got {sample_rate}")
    if isinstance(samples, np.ndarray) and samples.ndim != 1:
        raise ValueError("Analysis audio must be mono")
    return torch.from_numpy(np.asarray(samples, dtype=np.float32)), sample_rate


def run_silero(audio_tensor: torch.Tensor, sample_rate: int) -> tuple[list[VadSegment], dict[str, Any]]:
    model = load_silero_vad()
    timestamps = get_speech_timestamps(
        audio_tensor,
        model,
        sampling_rate=sample_rate,
        threshold=VAD_THRESHOLD,
        min_speech_duration_ms=MIN_SPEECH_MS,
        min_silence_duration_ms=MIN_SILENCE_MS,
        speech_pad_ms=SPEECH_PAD_MS,
        return_seconds=False,
    )

    segments: list[VadSegment] = []
    for index, segment in enumerate(timestamps):
        start_sample = int(segment["start"])
        end_sample = int(segment["end"])
        segments.append(
            VadSegment(
                id=f"vad-{index:04d}",
                start_sec=start_sample / sample_rate,
                end_sec=end_sample / sample_rate,
                start_sample=start_sample,
                end_sample=end_sample,
                backend="silero_vad",
                backend_version="6.x",
            )
        )

    summary = {
        "backend": "silero_vad",
        "segment_count": len(segments),
        "speech_duration_sec": round(sum(segment.end_sec - segment.start_sec for segment in segments), 3),
        "threshold": VAD_THRESHOLD,
        "min_speech_ms": MIN_SPEECH_MS,
        "min_silence_ms": MIN_SILENCE_MS,
        "speech_pad_ms": SPEECH_PAD_MS,
    }
    return segments, summary


def write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=True) + "\n", encoding="utf-8")


def window_ranges(duration_sec: float, window_sec: float, overlap_sec: float) -> list[tuple[float, float]]:
    if duration_sec <= window_sec:
        return [(0.0, duration_sec)]
    ranges: list[tuple[float, float]] = []
    cursor = 0.0
    step = window_sec - overlap_sec
    if step <= 0:
        raise ValueError("NEMO_WINDOW_SEC must be larger than NEMO_WINDOW_OVERLAP_SEC")
    while cursor < duration_sec:
        end = min(duration_sec, cursor + window_sec)
        ranges.append((cursor, end))
        if end >= duration_sec:
            break
        cursor += step
    return ranges


def vad_segments_for_window(vad_segments: list[VadSegment], start_sec: float, end_sec: float) -> list[VadSegment]:
    rows: list[VadSegment] = []
    for segment in vad_segments:
        clipped_start = max(segment.start_sec, start_sec)
        clipped_end = min(segment.end_sec, end_sec)
        if clipped_end <= clipped_start:
            continue
        local_start_sample = int(round((clipped_start - start_sec) * ANALYSIS_SAMPLE_RATE))
        local_end_sample = int(round((clipped_end - start_sec) * ANALYSIS_SAMPLE_RATE))
        rows.append(
            VadSegment(
                id=segment.id,
                start_sec=local_start_sample / ANALYSIS_SAMPLE_RATE,
                end_sec=local_end_sample / ANALYSIS_SAMPLE_RATE,
                start_sample=local_start_sample,
                end_sample=local_end_sample,
                backend=segment.backend,
                backend_version=segment.backend_version,
                confidence=segment.confidence,
            )
        )
    return rows


def write_nemo_external_vad_manifest(
    path: Path,
    audio_path: Path,
    vad_segments: list[VadSegment],
    uniq_id: str,
) -> None:
    rows = [
        {
            "audio_filepath": str(audio_path),
            "offset": round(segment.start_sec, 5),
            "duration": round(segment.end_sec - segment.start_sec, 5),
            "label": "UNK",
            "uniq_id": uniq_id,
        }
        for segment in vad_segments
        if segment.end_sample > segment.start_sample
    ]
    write_jsonl(path, rows)


def extract_audio_window(analysis_path: Path, window_path: Path, start_sec: float, end_sec: float) -> None:
    run(
        [
            "ffmpeg",
            "-y",
            "-ss",
            f"{start_sec:.3f}",
            "-i",
            str(analysis_path),
            "-t",
            f"{end_sec - start_sec:.3f}",
            "-ac",
            "1",
            "-ar",
            str(ANALYSIS_SAMPLE_RATE),
            "-c:a",
            "pcm_s16le",
            str(window_path),
        ]
    )


def parse_rttm_line(line: str, sample_rate: int, backend_version: str | None) -> SpeakerRegion:
    parts = line.strip().split()
    if len(parts) < 8:
        raise ValueError(f"Invalid RTTM line: {line!r}")
    start_sec = float(parts[3])
    duration_sec = float(parts[4])
    end_sec = start_sec + duration_sec
    speaker_label = parts[7]
    start_sample = int(round(start_sec * sample_rate))
    end_sample = int(round(end_sec * sample_rate))
    return SpeakerRegion(
        id=f"{speaker_label}-{start_sample}-{end_sample}",
        local_speaker_label=speaker_label,
        start_sec=start_sec,
        end_sec=end_sec,
        start_sample=start_sample,
        end_sample=end_sample,
        backend="nemo_clustering_diarizer",
        backend_version=backend_version,
        vad_source="silero_external",
        rfc_compliant=True,
        source="pred_rttm",
    )


def load_window_regions(summary: dict[str, Any], backend_version: str | None) -> list[dict[str, Any]]:
    pred_rttm_path = Path(summary["pred_rttm"])
    window_start = float(summary["start_sec"])
    rows: list[dict[str, Any]] = []
    for line in pred_rttm_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        region = asdict(parse_rttm_line(line, ANALYSIS_SAMPLE_RATE, backend_version))
        region["window_id"] = summary["id"]
        region["window_local_speaker_label"] = region["local_speaker_label"]
        region["start_sec"] += window_start
        region["end_sec"] += window_start
        region["start_sample"] = int(round(region["start_sec"] * ANALYSIS_SAMPLE_RATE))
        region["end_sample"] = int(round(region["end_sec"] * ANALYSIS_SAMPLE_RATE))
        region["source"] = str(pred_rttm_path)
        rows.append(region)
    return rows


def temporal_overlap(a: dict[str, Any], b: dict[str, Any], start_sec: float, end_sec: float) -> float:
    start = max(float(a["start_sec"]), float(b["start_sec"]), start_sec)
    end = min(float(a["end_sec"]), float(b["end_sec"]), end_sec)
    return max(0.0, end - start)


def stitch_window_speaker_labels(
    window_summaries: list[dict[str, Any]], backend_version: str | None
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    global_index = 0
    prev_regions: list[dict[str, Any]] = []
    prev_map: dict[str, str] = {}
    all_kept: list[dict[str, Any]] = []
    mappings: dict[str, dict[str, str]] = {}

    for window_index, summary in enumerate(window_summaries):
        if summary.get("status") != "ok":
            continue
        regions = load_window_regions(summary, backend_version)
        local_labels = sorted({row["window_local_speaker_label"] for row in regions})
        local_to_global: dict[str, str] = {}

        if window_index > 0 and prev_regions:
            overlap_start = float(summary["start_sec"])
            overlap_end = overlap_start + NEMO_WINDOW_OVERLAP_SEC
            scores: dict[tuple[str, str], float] = {}
            for current in regions:
                cur_label = current["window_local_speaker_label"]
                for previous in prev_regions:
                    prev_label = previous["window_local_speaker_label"]
                    overlap = temporal_overlap(previous, current, overlap_start, overlap_end)
                    if overlap > 0:
                        scores[(cur_label, prev_label)] = scores.get((cur_label, prev_label), 0.0) + overlap

            used_previous: set[str] = set()
            for cur_label in local_labels:
                candidates = [
                    (score, prev_label)
                    for (score_cur, prev_label), score in scores.items()
                    if score_cur == cur_label and prev_label not in used_previous
                ]
                if candidates:
                    score, prev_label = max(candidates)
                    if score >= 1.0 and prev_label in prev_map:
                        local_to_global[cur_label] = prev_map[prev_label]
                        used_previous.add(prev_label)

        for label in local_labels:
            if label not in local_to_global:
                local_to_global[label] = f"speaker_{global_index}"
                global_index += 1

        keep_start = float(summary["kept_start_sec"])
        keep_end = float(summary["kept_end_sec"])
        for region in regions:
            if region["end_sec"] <= keep_start or region["start_sec"] >= keep_end:
                continue
            region["start_sec"] = max(region["start_sec"], keep_start)
            region["end_sec"] = min(region["end_sec"], keep_end)
            region["start_sample"] = int(round(region["start_sec"] * ANALYSIS_SAMPLE_RATE))
            region["end_sample"] = int(round(region["end_sec"] * ANALYSIS_SAMPLE_RATE))
            mapped_label = local_to_global[region["window_local_speaker_label"]]
            region["local_speaker_label"] = mapped_label
            region["id"] = f"{mapped_label}-{region['start_sample']}-{region['end_sample']}"
            region["source"] = str(Path(region["source"]))
            all_kept.append(region)

        mappings[summary["id"]] = local_to_global
        prev_regions = regions
        prev_map = local_to_global

    mapping_summary = {
        "method": "adjacent_window_temporal_overlap",
        "min_overlap_sec": 1.0,
        "window_label_mappings": mappings,
    }
    return all_kept, mapping_summary


def run_nemo_window(
    analysis_path: Path,
    vad_segments: list[VadSegment],
    nemo_dir: Path,
    window_index: int,
    start_sec: float,
    end_sec: float,
    backend_version: str | None,
    ClusteringDiarizer: Any,
    NeuralDiarizerInferenceConfig: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    window_id = f"window_{window_index:03d}"
    window_dir = nemo_dir / window_id
    if window_dir.exists():
        shutil.rmtree(window_dir)
    ensure_dir(window_dir)

    window_audio_path = window_dir / f"{window_id}.wav"
    manifest_path = window_dir / "input_manifest.json"
    external_vad_manifest_path = window_dir / "external_vad_manifest.json"
    extract_audio_window(analysis_path, window_audio_path, start_sec, end_sec)

    local_vad_segments = vad_segments_for_window(vad_segments, start_sec, end_sec)
    if not local_vad_segments:
        return [], {
            "id": window_id,
            "start_sec": start_sec,
            "end_sec": end_sec,
            "status": "skipped_no_vad",
            "speaker_regions": 0,
        }

    write_manifest(
        manifest_path,
        {
            "audio_filepath": str(window_audio_path),
            "offset": 0.0,
            "duration": round(end_sec - start_sec, 5),
            "label": "infer",
            "text": "-",
            "num_speakers": None,
            "rttm_filepath": None,
            "uem_filepath": None,
            "ctm_filepath": None,
            "uniq_id": window_id,
        },
    )
    write_nemo_external_vad_manifest(external_vad_manifest_path, window_audio_path, local_vad_segments, window_id)

    cfg = NeuralDiarizerInferenceConfig()
    cfg.device = NEMO_DEVICE
    cfg.verbose = False
    cfg.batch_size = NEMO_BATCH_SIZE
    cfg.num_workers = 0
    cfg.sample_rate = ANALYSIS_SAMPLE_RATE
    cfg.diarizer.manifest_filepath = str(manifest_path)
    cfg.diarizer.out_dir = str(window_dir)
    cfg.diarizer.oracle_vad = False
    cfg.diarizer.vad.model_path = None
    cfg.diarizer.vad.external_vad_manifest = str(external_vad_manifest_path)
    cfg.diarizer.speaker_embeddings.model_path = NEMO_SPEAKER_MODEL
    cfg.diarizer.speaker_embeddings.parameters.window_length_in_sec = (1.5,)
    cfg.diarizer.speaker_embeddings.parameters.shift_length_in_sec = (0.75,)
    cfg.diarizer.speaker_embeddings.parameters.multiscale_weights = (1,)
    cfg.diarizer.speaker_embeddings.parameters.save_embeddings = NEMO_SAVE_EMBEDDINGS
    cfg.diarizer.clustering.parameters.oracle_num_speakers = False
    cfg.diarizer.clustering.parameters.max_num_speakers = NEMO_MAX_SPEAKERS

    diarizer = ClusteringDiarizer(cfg=cfg)
    diarizer.diarize()

    pred_rttm_path = window_dir / "pred_rttms" / f"{window_id}.rttm"
    if not pred_rttm_path.exists():
        raise FileNotFoundError(f"Expected NeMo RTTM output at {pred_rttm_path}")

    regions: list[dict[str, Any]] = []
    keep_start = start_sec if window_index == 0 else start_sec + (NEMO_WINDOW_OVERLAP_SEC / 2.0)
    keep_end = end_sec
    for line in pred_rttm_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        region = asdict(parse_rttm_line(line, ANALYSIS_SAMPLE_RATE, backend_version))
        region["start_sec"] += start_sec
        region["end_sec"] += start_sec
        region["start_sample"] = int(round(region["start_sec"] * ANALYSIS_SAMPLE_RATE))
        region["end_sample"] = int(round(region["end_sec"] * ANALYSIS_SAMPLE_RATE))
        region["id"] = f"{region['local_speaker_label']}-{region['start_sample']}-{region['end_sample']}"
        region["source"] = str(pred_rttm_path)
        if region["end_sec"] <= keep_start or region["start_sec"] >= keep_end:
            continue
        region["start_sec"] = max(region["start_sec"], keep_start)
        region["start_sample"] = int(round(region["start_sec"] * ANALYSIS_SAMPLE_RATE))
        regions.append(region)

    summary = {
        "id": window_id,
        "start_sec": start_sec,
        "end_sec": end_sec,
        "kept_start_sec": keep_start,
        "kept_end_sec": keep_end,
        "status": "ok",
        "speaker_regions": len(regions),
        "pred_rttm": str(pred_rttm_path),
        "external_vad_manifest": str(external_vad_manifest_path),
        "manifest": str(manifest_path),
        "vad_segment_count": len(local_vad_segments),
    }
    del diarizer
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    return regions, summary


def run_diarization_or_fallback(analysis_path: Path, vad_segments: list[VadSegment]) -> dict[str, Any]:
    try:
        from nemo.collections.asr.models import ClusteringDiarizer  # type: ignore  # pragma: no cover
        from nemo.collections.asr.models.configs.diarizer_config import (  # type: ignore  # pragma: no cover
            NeuralDiarizerInferenceConfig,
        )
        import nemo  # type: ignore  # pragma: no cover

        nemo_dir = OUTPUT_ROOT / "nemo"
        if nemo_dir.exists():
            shutil.rmtree(nemo_dir)
        ensure_dir(nemo_dir)

        duration_sec = duration_of(analysis_path)
        regions: list[dict[str, Any]] = []
        window_summaries: list[dict[str, Any]] = []
        for window_index, (start_sec, end_sec) in enumerate(
            window_ranges(duration_sec, NEMO_WINDOW_SEC, NEMO_WINDOW_OVERLAP_SEC)
        ):
            window_regions, window_summary = run_nemo_window(
                analysis_path=analysis_path,
                vad_segments=vad_segments,
                nemo_dir=nemo_dir,
                window_index=window_index,
                start_sec=start_sec,
                end_sec=end_sec,
                backend_version=getattr(nemo, "__version__", "unknown"),
                ClusteringDiarizer=ClusteringDiarizer,
                NeuralDiarizerInferenceConfig=NeuralDiarizerInferenceConfig,
            )
            regions.extend(window_regions)
            window_summaries.append(window_summary)

        stitched_regions, stitching_summary = stitch_window_speaker_labels(
            window_summaries, getattr(nemo, "__version__", "unknown")
        )
        speaker_labels = sorted({row["local_speaker_label"] for row in stitched_regions})
        return {
            "status": "ok",
            "backend": "nemo_clustering_diarizer_windowed",
            "backend_version": getattr(nemo, "__version__", "unknown"),
            "speaker_regions": stitched_regions,
            "speaker_count": len(speaker_labels),
            "speaker_labels": speaker_labels,
            "speaker_label_stitching": stitching_summary,
            "windows": window_summaries,
            "window_sec": NEMO_WINDOW_SEC,
            "window_overlap_sec": NEMO_WINDOW_OVERLAP_SEC,
            "device": NEMO_DEVICE,
            "speaker_model": NEMO_SPEAKER_MODEL,
            "batch_size": NEMO_BATCH_SIZE,
            "single_scale_embeddings": True,
            "rfc_compliant": True,
        }
    except Exception as exc:
        fallback_regions = [
            {
                "id": f"fallback-region-{index:04d}",
                "local_speaker_label": "unknown_speaker",
                "start_sec": segment.start_sec,
                "end_sec": segment.end_sec,
                "start_sample": segment.start_sample,
                "end_sample": segment.end_sample,
                "backend": "fallback_no_nemo",
                "vad_source": "silero_external",
                "reason_codes": ["nemo_unavailable", "diarization_not_rfc_compliant"],
            }
            for index, segment in enumerate(vad_segments)
        ]
        return {
            "status": "fallback_no_nemo",
            "backend": "fallback_no_nemo",
            "backend_version": None,
            "speaker_regions": fallback_regions,
            "reason": f"NeMo import failed: {type(exc).__name__}: {exc}",
            "rfc_compliant": False,
        }



def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def sec_to_sample(sec: float, sample_rate: int = ANALYSIS_SAMPLE_RATE) -> int:
    return int(round(sec * sample_rate))


def region_to_sample_interval(region: dict[str, Any]) -> dict[str, Any]:
    row = dict(region)
    row["start_sample"] = int(row.get("start_sample", sec_to_sample(float(row["start_sec"]))))
    row["end_sample"] = int(row.get("end_sample", sec_to_sample(float(row["end_sec"]))))
    row["id"] = row.get("id") or f"{row.get('local_speaker_label', 'speaker')}-{row['start_sample']}-{row['end_sample']}"
    return row


def overlaps(start_a: int, end_a: int, start_b: int, end_b: int) -> bool:
    return start_a < end_b and start_b < end_a


def has_non_target_intrusion(gap_start: int, gap_end: int, non_target_regions: list[dict[str, Any]]) -> bool:
    if gap_end <= gap_start:
        return False
    return any(overlaps(gap_start, gap_end, row["start_sample"], row["end_sample"]) for row in non_target_regions)


def merge_target_regions(speaker_regions: list[dict[str, Any]], target_speaker: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    sampled = [region_to_sample_interval(row) for row in speaker_regions]
    target_regions = sorted(
        [row for row in sampled if row.get("local_speaker_label") == target_speaker], key=lambda row: row["start_sample"]
    )
    non_target_regions = [row for row in sampled if row.get("local_speaker_label") != target_speaker]
    trusted: list[dict[str, Any]] = []
    non_target_blocked_merges = 0
    clean_gap_merges = 0

    for region in target_regions:
        if region["end_sample"] <= region["start_sample"]:
            continue
        if not trusted:
            trusted.append(
                {
                    "trusted_region_id": f"trusted_region_{len(trusted):06d}",
                    "start_sample": region["start_sample"],
                    "end_sample": region["end_sample"],
                    "included_region_ids": [region["id"]],
                }
            )
            continue

        previous = trusted[-1]
        if not has_non_target_intrusion(previous["end_sample"], region["start_sample"], non_target_regions):
            previous["end_sample"] = max(previous["end_sample"], region["end_sample"])
            previous["included_region_ids"].append(region["id"])
            clean_gap_merges += 1
        else:
            non_target_blocked_merges += 1
            trusted.append(
                {
                    "trusted_region_id": f"trusted_region_{len(trusted):06d}",
                    "start_sample": region["start_sample"],
                    "end_sample": region["end_sample"],
                    "included_region_ids": [region["id"]],
                }
            )

    return trusted, {
        "target_regions": len(target_regions),
        "non_target_regions": len(non_target_regions),
        "clean_gap_merges": clean_gap_merges,
        "non_target_blocked_merges": non_target_blocked_merges,
    }


def vad_speech_intervals(vad_segments: list[dict[str, Any]]) -> list[tuple[int, int]]:
    intervals = []
    for segment in vad_segments:
        start = int(segment.get("start_sample", sec_to_sample(float(segment["start_sec"]))))
        end = int(segment.get("end_sample", sec_to_sample(float(segment["end_sec"]))))
        if end > start:
            intervals.append((start, end))
    return sorted(intervals)


def vad_gaps_in_range(start_sample: int, end_sample: int, speech_intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    cursor = start_sample
    gaps: list[tuple[int, int]] = []
    for speech_start, speech_end in speech_intervals:
        if speech_end <= start_sample:
            continue
        if speech_start >= end_sample:
            break
        clipped_speech_start = max(speech_start, start_sample)
        clipped_speech_end = min(speech_end, end_sample)
        if clipped_speech_start > cursor:
            gaps.append((cursor, clipped_speech_start))
        cursor = max(cursor, clipped_speech_end)
    if cursor < end_sample:
        gaps.append((cursor, end_sample))
    return gaps


def padded_duration_samples(chunk_start: int, chunk_end: int, audio_len: int) -> int:
    pad_samples = sec_to_sample(PROCESSING_BUFFER_PAD_SEC)
    processing_start = max(0, chunk_start - pad_samples)
    processing_end = min(audio_len, chunk_end + pad_samples)
    return processing_end - processing_start


def latest_feasible_split_sample(chunk_start: int, region_end: int, audio_len: int) -> int:
    max_buffer_samples = sec_to_sample(MAX_PROCESSING_BUFFER_SEC)
    low = chunk_start + 1
    high = min(region_end, audio_len)
    while low < high:
        candidate = (low + high + 1) // 2
        if padded_duration_samples(chunk_start, candidate, audio_len) < max_buffer_samples:
            low = candidate
        else:
            high = candidate - 1
    return low


def choose_split_sample(
    chunk_start: int,
    region_end: int,
    speech_intervals: list[tuple[int, int]],
    audio: np.ndarray,
    target_chunk_samples: int,
    search_window_samples: int,
    min_split_gap_samples: int,
) -> tuple[int, str, list[str]]:
    desired = min(chunk_start + target_chunk_samples, region_end)
    widening_passes = [
        ("near_target", 2.0, 2.0),
        ("widened_4s", 4.0, 4.0),
        ("widened_8s", 8.0, 4.0),
    ]
    for strategy_suffix, before_sec, after_sec in widening_passes:
        search_start = max(chunk_start, desired - sec_to_sample(before_sec))
        search_end = min(region_end, desired + sec_to_sample(after_sec))
        gaps = [
            (start, end)
            for start, end in vad_gaps_in_range(search_start, search_end, speech_intervals)
            if end - start >= min_split_gap_samples
        ]
        if not gaps:
            continue
        containing = [(start, end) for start, end in gaps if start <= desired <= end]
        candidates = containing or gaps
        best_start, best_end = sorted(
            candidates,
            key=lambda gap: (
                0 if gap[0] <= desired <= gap[1] else min(abs(desired - gap[0]), abs(desired - gap[1])),
                -(gap[1] - gap[0]),
            ),
        )[0]
        centers, levels = frame_rms_db(audio[best_start:best_end])
        if len(centers):
            return best_start + int(centers[int(np.argmin(levels))]), f"vad_gap_rms_valley_{strategy_suffix}", []
        return (best_start + best_end) // 2, f"vad_gap_midpoint_fallback_{strategy_suffix}", [
            "missing_rms_frame_in_vad_gap",
        ]

    forced_search_start = chunk_start + sec_to_sample(15.0)
    forced_search_end = latest_feasible_split_sample(chunk_start, region_end, len(audio))
    centers, levels = frame_rms_db(audio[forced_search_start:forced_search_end])
    if len(centers):
        return forced_search_start + int(centers[int(np.argmin(levels))]), "forced_rms_valley_full_feasible_range", [
            "provisional_pre_mfa_split",
            "forced_chunk_split",
        ]
    return desired, "forced_target_fallback", [
        "provisional_pre_mfa_split",
        "forced_chunk_split",
        "missing_rms_frame_for_forced_split",
    ]


def split_trusted_regions(
    trusted_regions: list[dict[str, Any]],
    speech_intervals: list[tuple[int, int]],
    audio: np.ndarray,
    target_chunk_samples: int,
) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    search_window_samples = sec_to_sample(SPLIT_SEARCH_WINDOW_SEC)
    min_split_gap_samples = sec_to_sample(MIN_SPLIT_GAP_SEC)
    for region in trusted_regions:
        cursor = region["start_sample"]
        left_provisional_boundary = False
        while region["end_sample"] - cursor > target_chunk_samples:
            split_sample, split_strategy, reason_codes = choose_split_sample(
                cursor,
                region["end_sample"],
                speech_intervals,
                audio,
                target_chunk_samples,
                search_window_samples,
                min_split_gap_samples,
            )
            if split_sample <= cursor:
                split_sample = min(region["end_sample"], cursor + target_chunk_samples)
                split_strategy = "forced_target_fallback"
                reason_codes = ["provisional_pre_mfa_split", "forced_chunk_split", "non_advancing_split_guard"]
            chunks.append(
                {
                    "trusted_region_id": region["trusted_region_id"],
                    "trusted_start_sample": cursor,
                    "trusted_end_sample": split_sample,
                    "included_region_ids": region["included_region_ids"],
                    "split_strategy": split_strategy,
                    "reason_codes": reason_codes,
                    "left_provisional_boundary": left_provisional_boundary,
                    "right_provisional_boundary": "provisional_pre_mfa_split" in reason_codes,
                }
            )
            cursor = split_sample
            left_provisional_boundary = "provisional_pre_mfa_split" in reason_codes
        if region["end_sample"] > cursor:
            chunks.append(
                {
                    "trusted_region_id": region["trusted_region_id"],
                    "trusted_start_sample": cursor,
                    "trusted_end_sample": region["end_sample"],
                    "included_region_ids": region["included_region_ids"],
                    "split_strategy": "tail" if cursor != region["start_sample"] else "whole_region",
                    "reason_codes": [],
                    "left_provisional_boundary": left_provisional_boundary,
                    "right_provisional_boundary": False,
                }
            )
    return chunks


def assert_chunks_do_not_overlap(chunks: list[dict[str, Any]]) -> None:
    previous_end: int | None = None
    for chunk in sorted(chunks, key=lambda row: row["trusted_start_sample"]):
        if previous_end is not None and chunk["trusted_start_sample"] < previous_end:
            raise RuntimeError("trusted chunks overlap")
        previous_end = chunk["trusted_end_sample"]


def build_processing_buffers(
    analysis_path: Path,
    vad_path: Path,
    speaker_regions_path: Path,
    output_root: Path,
    target_speaker: str = TARGET_SPEAKER_LABEL,
) -> dict[str, Any]:
    audio, sample_rate = sf.read(str(analysis_path), dtype="float32", always_2d=False)
    if sample_rate != ANALYSIS_SAMPLE_RATE:
        raise ValueError(f"Expected {ANALYSIS_SAMPLE_RATE} Hz analysis audio, got {sample_rate}")
    if isinstance(audio, np.ndarray) and audio.ndim != 1:
        raise ValueError("Processing buffer source must be mono analysis audio")
    audio = np.asarray(audio, dtype=np.float32)

    vad_segments = load_jsonl(vad_path)
    speaker_regions = load_jsonl(speaker_regions_path)
    target_chunk_samples = sec_to_sample(TARGET_CHUNK_SEC)
    pad_samples = sec_to_sample(PROCESSING_BUFFER_PAD_SEC)
    max_buffer_samples = sec_to_sample(MAX_PROCESSING_BUFFER_SEC)

    trusted_regions, merge_summary = merge_target_regions(speaker_regions, target_speaker)
    speech_intervals = vad_speech_intervals(vad_segments)
    trusted_chunks = split_trusted_regions(trusted_regions, speech_intervals, audio, target_chunk_samples)
    assert_chunks_do_not_overlap(trusted_chunks)

    buffers_dir = output_root / "buffers"
    if buffers_dir.exists():
        shutil.rmtree(buffers_dir)
    ensure_dir(buffers_dir)

    buffers: list[dict[str, Any]] = []
    for index, chunk in enumerate(trusted_chunks):
        trusted_start = int(chunk["trusted_start_sample"])
        trusted_end = int(chunk["trusted_end_sample"])
        source_start = max(0, trusted_start - pad_samples)
        source_end = min(len(audio), trusted_end + pad_samples)
        duration_samples = source_end - source_start
        if duration_samples >= max_buffer_samples:
            raise RuntimeError(
                f"processing buffer too long: buffer_{index:06d} has {duration_samples} samples "
                f"({duration_samples / sample_rate:.3f}s)"
            )
        if source_start < 0 or source_end > len(audio) or source_start >= source_end:
            raise RuntimeError(f"invalid buffer bounds for buffer_{index:06d}")
        if trusted_start < source_start or trusted_end > source_end:
            raise RuntimeError(f"trusted bounds outside source bounds for buffer_{index:06d}")

        buffer_id = f"buffer_{index:06d}"
        audio_rel_path = Path("buffers") / f"{buffer_id}.wav"
        audio_path = output_root / audio_rel_path
        sf.write(str(audio_path), audio[source_start:source_end], sample_rate, subtype="PCM_16")
        buffers.append(
            {
                "buffer_id": buffer_id,
                "audio_path": str(audio_rel_path),
                "source_start_sample": source_start,
                "source_end_sample": source_end,
                "source_start_sec": source_start / sample_rate,
                "source_end_sec": source_end / sample_rate,
                "trusted_start_sample": trusted_start,
                "trusted_end_sample": trusted_end,
                "trusted_start_sec": trusted_start / sample_rate,
                "trusted_end_sec": trusted_end / sample_rate,
                "trusted_local_start_sample": trusted_start - source_start,
                "trusted_local_end_sample": trusted_end - source_start,
                "duration_samples": duration_samples,
                "duration_sec": duration_samples / sample_rate,
                "target_speaker": target_speaker,
                "trusted_region_id": chunk["trusted_region_id"],
                "included_region_ids": chunk["included_region_ids"],
                "split_strategy": chunk["split_strategy"],
                "reason_codes": chunk["reason_codes"],
                "left_provisional_boundary": chunk["left_provisional_boundary"],
                "right_provisional_boundary": chunk["right_provisional_boundary"],
            }
        )

    strategy_counts: dict[str, int] = {}
    forced_splits = 0
    for buffer in buffers:
        strategy_counts[buffer["split_strategy"]] = strategy_counts.get(buffer["split_strategy"], 0) + 1
        if "forced_chunk_split" in buffer["reason_codes"]:
            forced_splits += 1
    durations = sorted(buffer["duration_sec"] for buffer in buffers)
    max_duration = durations[-1] if durations else 0.0
    all_under_limit = all(buffer["duration_samples"] < max_buffer_samples for buffer in buffers)
    summary = {
        "target_speaker": target_speaker,
        "analysis_audio": str(analysis_path),
        "sample_rate": sample_rate,
        "audio_duration_sec": len(audio) / sample_rate,
        "target_regions": merge_summary["target_regions"],
        "non_target_regions": merge_summary["non_target_regions"],
        "merged_trusted_regions": len(trusted_regions),
        "trusted_chunks": len(trusted_chunks),
        "processing_buffers": len(buffers),
        "max_buffer_duration_sec": round(max_duration, 6),
        "min_buffer_duration_sec": round(durations[0], 6) if durations else 0.0,
        "p50_buffer_duration_sec": round(durations[len(durations) // 2], 6) if durations else 0.0,
        "p90_buffer_duration_sec": round(durations[int(len(durations) * 0.90)], 6) if durations else 0.0,
        "buffers_under_1_sec": sum(duration < 1.0 for duration in durations),
        "buffers_under_3_sec": sum(duration < 3.0 for duration in durations),
        "buffers_under_5_sec": sum(duration < 5.0 for duration in durations),
        "all_buffers_under_29_5_sec": all_under_limit,
        "forced_splits": forced_splits,
        "vad_gap_splits": sum(count for strategy, count in strategy_counts.items() if strategy.startswith("vad_gap")),
        "split_strategy_counts": strategy_counts,
        "merge_summary": merge_summary,
        "constants": {
            "target_chunk_sec": TARGET_CHUNK_SEC,
            "max_processing_buffer_sec": MAX_PROCESSING_BUFFER_SEC,
            "processing_buffer_pad_sec": PROCESSING_BUFFER_PAD_SEC,
            "split_search_window_sec": SPLIT_SEARCH_WINDOW_SEC,
            "min_split_gap_sec": MIN_SPLIT_GAP_SEC,
            "provisional_split_guard_sec": PROVISIONAL_SPLIT_GUARD_SEC,
        },
    }
    write_json(output_root / "processing_buffers.json", buffers)
    write_json(output_root / "processing_buffer_summary.json", summary)

    print("target speaker:", target_speaker)
    print("target regions:", merge_summary["target_regions"])
    print("merged trusted regions:", len(trusted_regions))
    print("trusted chunks:", len(trusted_chunks))
    print("processing buffers:", len(buffers))
    print("max buffer duration:", f"{max_duration:.3f}s")
    print("forced splits:", forced_splits)
    print("vad-gap RMS-valley splits:", sum(count for strategy, count in strategy_counts.items() if strategy.startswith("vad_gap")))
    print("all buffers < 29.5s:", "yes" if all_under_limit else "no")
    return summary


def hazard_reason_codes(symbols: list[str], contains_numeric: bool) -> list[str]:
    reasons = ["contains_numeric_token"] if contains_numeric else []
    if symbols:
        reasons.append("contains_danger_symbol")
    if any(symbol in "$€£¥" for symbol in symbols):
        reasons.append("contains_currency_symbol")
    if "%" in symbols:
        reasons.append("contains_percent_symbol")
    return sorted(set(reasons))


def classify_raw_token(raw: str, index: int) -> dict[str, Any]:
    symbols = sorted({character for character in raw if character in DANGER_SYMBOLS})
    contains_numeric = any(character.isdigit() for character in raw)
    alignment_tokens = [
        token.strip("'")
        for token in re.sub(r"[^a-z0-9'\\s]", " ", raw.lower()).split()
        if token.strip("'")
    ]
    reasons = hazard_reason_codes(symbols, contains_numeric)
    symbol_only = bool(symbols) and not alignment_tokens
    if symbol_only:
        reasons.append("symbol_only_token_stripped")
    return {
        "id": f"raw-{index:04d}",
        "index": index,
        "raw": raw,
        "alignment_tokens": alignment_tokens,
        "contains_numeric": contains_numeric,
        "contains_danger_symbol": bool(symbols),
        "danger_symbols": symbols,
        "reason_codes": sorted(set(reasons)),
    }


def normalize_for_mfa(text: str) -> dict[str, Any]:
    raw_asr_text = text
    raw_tokens = re.findall(r"\S+", text)
    token_hazards = [classify_raw_token(raw, index) for index, raw in enumerate(raw_tokens)]
    alignment_token_rows: list[dict[str, Any]] = []
    for raw_token in token_hazards:
        for emitted_index, token in enumerate(raw_token["alignment_tokens"]):
            alignment_token_rows.append(
                {
                    "index": len(alignment_token_rows),
                    "raw_token_id": raw_token["id"],
                    "raw_token_index": raw_token["index"],
                    "emitted_index": emitted_index,
                    "raw": raw_token["raw"],
                    "alignment": token,
                    "contains_numeric": raw_token["contains_numeric"],
                    "contains_danger_symbol": raw_token["contains_danger_symbol"],
                    "danger_symbols": raw_token["danger_symbols"],
                    "reason_codes": raw_token["reason_codes"],
                }
            )
    alignment_tokens = [row["alignment"] for row in alignment_token_rows]
    alignment_text = " ".join(alignment_tokens)
    numeric_tokens = [row["raw"] for row in token_hazards if row["contains_numeric"]]
    symbols = sorted({symbol for row in token_hazards for symbol in row["danger_symbols"]})
    reason_codes = sorted({reason for row in token_hazards for reason in row["reason_codes"]})
    if not alignment_text:
        reason_codes.append("empty_normalized_transcript")
    if any("symbol_only_token_stripped" in row["reason_codes"] for row in token_hazards):
        reason_codes.extend(["buffer_requires_review", "disable_automatic_cutpoints"])
    return {
        "original_text": raw_asr_text,
        "raw_asr_text": raw_asr_text,
        "training_text": raw_asr_text,
        "alignment_text": alignment_text,
        "normalized_text": alignment_text,
        "tokens": alignment_tokens,
        "alignment_tokens": alignment_token_rows,
        "token_hazards": token_hazards,
        "excluded": symbols,
        "numeric_tokens": numeric_tokens,
        "symbols": symbols,
        "needs_review": bool(reason_codes),
        "disable_automatic_cutpoints": "disable_automatic_cutpoints" in reason_codes,
        "reason_codes": sorted(set(reason_codes)),
    }


def build_asr_mfa_queue(output_root: Path) -> dict[str, Any]:
    buffers = load_json(output_root / "processing_buffers.json")
    queue: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    held: list[dict[str, Any]] = []
    queue_dir = output_root / "asr_mfa_queue"
    if queue_dir.exists():
        shutil.rmtree(queue_dir)
    ensure_dir(queue_dir)
    for buffer in buffers:
        row = dict(buffer)
        if row["duration_sec"] < MIN_ASR_MFA_BUFFER_SEC:
            row["queue_status"] = "rejected"
            row["queue_reason_codes"] = ["buffer_under_5_sec"]
            rejected.append(row)
            continue
        queue_audio_path = queue_dir / Path(row["audio_path"]).name
        shutil.copy2(output_root / row["audio_path"], queue_audio_path)
        row["queue_status"] = "asr_mfa_ready"
        row["queue_reason_codes"] = (
            ["provisional_pre_mfa_split_review"]
            if "forced_chunk_split" in row["reason_codes"]
            else []
        )
        row["queue_audio_path"] = str(queue_audio_path.relative_to(output_root))
        queue.append(row)
    write_json(output_root / "asr_mfa_queue.json", queue)
    write_json(output_root / "rejected_buffers.json", rejected)
    write_json(output_root / "held_buffers.json", held)
    summary = {
        "ready_buffers": len(queue),
        "rejected_buffers": len(rejected),
        "held_buffers": len(held),
        "ready_duration_sec": sum(row["duration_sec"] for row in queue),
        "min_ready_buffer_sec": MIN_ASR_MFA_BUFFER_SEC,
    }
    write_json(output_root / "asr_mfa_queue_summary.json", summary)
    return summary


def run_asr_for_queue(output_root: Path, limit: int | None = None) -> dict[str, Any]:
    from faster_whisper import WhisperModel  # type: ignore

    queue_path = output_root / "asr_mfa_queue.json"
    if not queue_path.exists():
        raise FileNotFoundError(f"Missing ASR/MFA queue: {queue_path}")
    queue = load_json(queue_path)
    if limit is not None:
        queue = queue[:limit]

    model = WhisperModel(
        FASTER_WHISPER_MODEL,
        device=FASTER_WHISPER_DEVICE,
        compute_type=FASTER_WHISPER_COMPUTE_TYPE,
    )
    transcripts: list[dict[str, Any]] = []
    for index, buffer in enumerate(queue, start=1):
        audio_path = output_root / buffer.get("queue_audio_path", buffer["audio_path"])
        segments, info = model.transcribe(
            str(audio_path),
            language="en",
            task="transcribe",
            vad_filter=False,
            word_timestamps=False,
            condition_on_previous_text=False,
            beam_size=5,
        )
        segment_rows = [
            {
                "id": segment.id,
                "seek": segment.seek,
                "start": segment.start,
                "end": segment.end,
                "text": segment.text,
                "avg_logprob": segment.avg_logprob,
                "compression_ratio": segment.compression_ratio,
                "no_speech_prob": segment.no_speech_prob,
            }
            for segment in segments
        ]
        text = " ".join(row["text"].strip() for row in segment_rows).strip()
        transcripts.append(
            {
                "buffer_id": buffer["buffer_id"],
                "audio_path": str(audio_path.relative_to(output_root)),
                "text": text,
                "segments": segment_rows,
                "language": info.language,
                "language_probability": info.language_probability,
                "duration": info.duration,
                "duration_after_vad": getattr(info, "duration_after_vad", None),
                "asr_backend": "faster-whisper",
                "asr_model": FASTER_WHISPER_MODEL,
                "device": FASTER_WHISPER_DEVICE,
                "compute_type": FASTER_WHISPER_COMPUTE_TYPE,
                "reason_codes": [] if text else ["empty_asr_transcript"],
            }
        )
        print(f"asr {index}/{len(queue)} {buffer['buffer_id']} chars={len(text)}")

    write_json(output_root / "transcripts.json", transcripts)
    summary = {
        "backend": "faster-whisper",
        "model": FASTER_WHISPER_MODEL,
        "device": FASTER_WHISPER_DEVICE,
        "compute_type": FASTER_WHISPER_COMPUTE_TYPE,
        "buffer_count": len(transcripts),
        "empty_transcripts": sum(not row["text"] for row in transcripts),
        "total_chars": sum(len(row["text"]) for row in transcripts),
    }
    write_json(output_root / "transcripts_summary.json", summary)
    return summary


def normalize_transcripts(output_root: Path) -> dict[str, Any]:
    transcripts = load_json(output_root / "transcripts.json")
    normalized_rows: list[dict[str, Any]] = []
    for row in transcripts:
        normalized = normalize_for_mfa(row["text"])
        normalized_rows.append(
            {
                "buffer_id": row["buffer_id"],
                "audio_path": row["audio_path"],
                **normalized,
            }
        )
    write_json(output_root / "normalized_transcripts.json", normalized_rows)
    write_json(output_root / "transcript_hazards.json", normalized_rows)
    symbol_counts = {
        symbol: sum(symbol in row["symbols"] for row in normalized_rows)
        for symbol in sorted({symbol for row in normalized_rows for symbol in row["symbols"]})
    }
    hazard_summary = {
        "buffer_count": len(normalized_rows),
        "buffers_with_symbol_hazards": sum(bool(row["symbols"]) for row in normalized_rows),
        "buffers_with_numeric_tokens": sum(bool(row["numeric_tokens"]) for row in normalized_rows),
        "buffers_requiring_review": sum(row["needs_review"] for row in normalized_rows),
        "buffers_with_automatic_cutpoints_disabled": sum(
            row["disable_automatic_cutpoints"] for row in normalized_rows
        ),
        "symbol_counts": symbol_counts,
        "numeric_token_count": sum(len(row["numeric_tokens"]) for row in normalized_rows),
        "symbol_only_tokens": sum(
            "symbol_only_token_stripped" in token["reason_codes"]
            for row in normalized_rows
            for token in row["token_hazards"]
        ),
    }
    write_json(output_root / "symbol_hazard_summary.json", hazard_summary)
    summary = {
        "buffer_count": len(normalized_rows),
        "empty_normalized_transcripts": sum(not row["normalized_text"] for row in normalized_rows),
        "total_tokens": sum(len(row["tokens"]) for row in normalized_rows),
        **hazard_summary,
    }
    write_json(output_root / "normalization_summary.json", summary)
    return summary


def prepare_mfa_corpus(output_root: Path) -> dict[str, Any]:
    normalized_rows = load_json(output_root / "normalized_transcripts.json")
    corpus_dir = output_root / "mfa_corpus"
    if corpus_dir.exists():
        shutil.rmtree(corpus_dir)
    ensure_dir(corpus_dir)

    prepared: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in normalized_rows:
        if not row["normalized_text"]:
            skipped.append({"buffer_id": row["buffer_id"], "reason_codes": ["empty_normalized_transcript"]})
            continue
        src_audio = output_root / row["audio_path"]
        wav_path = corpus_dir / f"{row['buffer_id']}.wav"
        lab_path = corpus_dir / f"{row['buffer_id']}.lab"
        shutil.copy2(src_audio, wav_path)
        lab_path.write_text(row["normalized_text"] + "\n", encoding="utf-8")
        prepared.append(
            {
                "buffer_id": row["buffer_id"],
                "wav_path": str(wav_path.relative_to(output_root)),
                "lab_path": str(lab_path.relative_to(output_root)),
                "normalized_text": row["normalized_text"],
                "token_count": len(row["tokens"]),
            }
        )
    write_json(output_root / "mfa_corpus_manifest.json", prepared)
    summary = {
        "prepared_count": len(prepared),
        "skipped_count": len(skipped),
        "skipped": skipped,
        "corpus_dir": str(corpus_dir),
    }
    write_json(output_root / "mfa_corpus_summary.json", summary)
    return summary


def run_mfa_alignment(output_root: Path) -> dict[str, Any]:
    mfa = shutil.which("mfa")
    corpus_dir = output_root / "mfa_corpus"
    output_dir = output_root / "mfa_output"
    if output_dir.exists():
        shutil.rmtree(output_dir)
    ensure_dir(output_dir)
    if mfa is None:
        summary = {
            "status": "missing_mfa_command",
            "reason": "Montreal Forced Aligner command `mfa` is not installed on PATH.",
            "corpus_dir": str(corpus_dir),
            "output_dir": str(output_dir),
        }
        write_json(output_root / "mfa_summary.json", summary)
        return summary

    cmd = [
        mfa,
        "align",
        "--clean",
        "--overwrite",
        str(corpus_dir),
        "english_us_mfa",
        "english_mfa",
        str(output_dir),
    ]
    completed = subprocess.run(cmd, text=True, capture_output=True)
    summary = {
        "status": "ok" if completed.returncode == 0 else "failed",
        "returncode": completed.returncode,
        "command": cmd,
        "stdout_tail": completed.stdout[-4000:],
        "stderr_tail": completed.stderr[-4000:],
        "corpus_dir": str(corpus_dir),
        "output_dir": str(output_dir),
    }
    write_json(output_root / "mfa_summary.json", summary)
    if completed.returncode != 0:
        return summary
    parse_mfa_textgrids(output_root)
    return summary


def parse_mfa_textgrids(output_root: Path) -> dict[str, Any]:
    from praatio import textgrid  # type: ignore

    queue_by_id = {row["buffer_id"]: row for row in load_json(output_root / "asr_mfa_queue.json")}
    normalized_by_id = {row["buffer_id"]: row for row in load_json(output_root / "normalized_transcripts.json")}
    oov_words = persist_mfa_oov_words(output_root)
    output_dir = output_root / "mfa_output"
    rows: list[dict[str, Any]] = []
    for tg_path in sorted(output_dir.rglob("*.TextGrid")):
        buffer_id = tg_path.stem
        buffer = queue_by_id.get(buffer_id)
        if buffer is None:
            continue
        tg = textgrid.openTextgrid(str(tg_path), includeEmptyIntervals=False)
        tier_name = "words" if "words" in tg.tierNames else tg.tierNames[0]
        tier = tg.getTier(tier_name)
        entries = [entry for entry in tier.entries if entry.label.strip()]
        alignment_tokens = normalized_by_id.get(buffer_id, {}).get("alignment_tokens", [])
        mapping_mismatch = len(entries) != len(alignment_tokens)
        if not mapping_mismatch:
            mapping_mismatch = any(
                entry.label.strip().lower() != token["alignment"]
                for entry, token in zip(entries, alignment_tokens)
            )
        for index, entry in enumerate(entries):
            start = float(entry.start)
            end = float(entry.end)
            label = entry.label.strip()
            if not label:
                continue
            source_start_sample = int(buffer["source_start_sample"] + round(start * ANALYSIS_SAMPLE_RATE))
            source_end_sample = int(buffer["source_start_sample"] + round(end * ANALYSIS_SAMPLE_RATE))
            hazard = alignment_tokens[index] if not mapping_mismatch else {}
            is_oov = label.lower() in oov_words
            review_reasons = list(hazard.get("reason_codes", []))
            if is_oov:
                review_reasons.extend(["contains_oov", "requires_transcript_review"])
            if is_oov and re.fullmatch(r"\d+(?:[.,]\d+)?", label):
                review_reasons.append("contains_numeric_oov")
            if mapping_mismatch:
                review_reasons.extend(
                    ["alignment_token_word_mismatch", "buffer_requires_review", "disable_automatic_cutpoints"]
                )
            rows.append(
                {
                    "id": f"{buffer_id}-word-{index:04d}",
                    "buffer_id": buffer_id,
                    "word": label,
                    "is_oov": is_oov,
                    "is_numeric_oov": is_oov and bool(re.fullmatch(r"\d+(?:[.,]\d+)?", label)),
                    "raw_token_id": hazard.get("raw_token_id"),
                    "raw_token": hazard.get("raw"),
                    "contains_numeric": bool(hazard.get("contains_numeric")),
                    "contains_danger_symbol": bool(hazard.get("contains_danger_symbol")),
                    "danger_symbols": hazard.get("danger_symbols", []),
                    "alignment_token_word_mismatch": mapping_mismatch,
                    "review_reason_codes": sorted(set(review_reasons)),
                    "buffer_start_sec": start,
                    "buffer_end_sec": end,
                    "source_start_sample": source_start_sample,
                    "source_end_sample": source_end_sample,
                    "source_start_sec": source_start_sample / ANALYSIS_SAMPLE_RATE,
                    "source_end_sec": source_end_sample / ANALYSIS_SAMPLE_RATE,
                    "textgrid": str(tg_path.relative_to(output_root)),
                }
            )
    write_jsonl(output_root / "aligned_words.jsonl", rows)
    summary = {
        "textgrid_count": len(list(output_dir.rglob("*.TextGrid"))),
        "aligned_word_count": len(rows),
        "oov_word_count": sum(row["is_oov"] for row in rows),
        "numeric_oov_word_count": sum(row["is_numeric_oov"] for row in rows),
        "words_with_symbol_hazards": sum(row["contains_danger_symbol"] for row in rows),
        "words_with_numeric_hazards": sum(row["contains_numeric"] for row in rows),
        "buffers_with_alignment_token_word_mismatch": len(
            {row["buffer_id"] for row in rows if row["alignment_token_word_mismatch"]}
        ),
    }
    write_json(output_root / "aligned_words_summary.json", summary)
    return summary


def parse_oov_count_file(path: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.strip().split()
        if not parts:
            continue
        if len(parts) == 1:
            counts[parts[0].lower()] = max(counts.get(parts[0].lower(), 0), 1)
            continue
        if parts[-1].isdigit():
            word = " ".join(parts[:-1]).lower()
            counts[word] = max(counts.get(word, 0), int(parts[-1]))
        elif parts[0].isdigit():
            word = " ".join(parts[1:]).lower()
            counts[word] = max(counts.get(word, 0), int(parts[0]))
    return counts


def discover_mfa_oov_artifacts(output_root: Path) -> dict[str, list[Path]]:
    search_roots = [output_root, MFA_WORK_ROOT]
    names = {
        "oov_counts": ["oov_counts*.txt"],
        "oovs_found": ["oovs_found*.txt"],
        "utterance_oovs": ["utterance_oovs*.txt"],
        "normalize_oov_log": ["normalize_oov.log"],
    }
    discovered: dict[str, list[Path]] = {kind: [] for kind in names}
    for root in search_roots:
        if not root.exists():
            continue
        for kind, patterns in names.items():
            for pattern in patterns:
                for path in root.rglob(pattern):
                    if output_root / "mfa_oov_artifacts" in path.parents:
                        continue
                    if path not in discovered[kind]:
                        discovered[kind].append(path)
    for paths in discovered.values():
        paths.sort(key=lambda path: (output_root not in path.parents, -path.stat().st_mtime))
    return discovered


def snapshot_oov_artifacts(output_root: Path, discovered: dict[str, list[Path]]) -> list[str]:
    artifact_dir = output_root / "mfa_oov_artifacts"
    ensure_dir(artifact_dir)
    copied: list[str] = []
    for kind, paths in discovered.items():
        for index, source in enumerate(paths):
            destination = artifact_dir / f"{kind}_{index:02d}_{source.name}"
            if source.resolve() == destination.resolve():
                continue
            shutil.copy2(source, destination)
            copied.append(str(destination.relative_to(output_root)))
    return copied


def expected_mfa_oov_counts(output_root: Path) -> tuple[int | None, int | None]:
    summary_path = output_root / "mfa_summary.json"
    if not summary_path.exists():
        return None, None
    summary = load_json(summary_path)
    text = "\n".join(
        str(summary.get(key, "")) for key in ("stdout_tail", "stderr_tail")
    )
    type_match = re.search(r"(\d+)\s+OOV word types", text)
    token_match = re.search(r"(\d+)\s*total OOV tokens", text)
    return (
        int(type_match.group(1)) if type_match else summary.get("oov_word_types"),
        int(token_match.group(1)) if token_match else summary.get("oov_tokens"),
    )


def persist_mfa_oov_words(output_root: Path) -> set[str]:
    existing_path = output_root / "oov_words.json"
    if existing_path.exists():
        existing = load_json(existing_path)
        return {row["word"] if isinstance(row, dict) else row for row in existing}

    discovered = discover_mfa_oov_artifacts(output_root)
    oov_counts: dict[str, int] = {}
    parser = None
    source_files: list[str] = []
    selected: dict[str, list[Path]] = {kind: [] for kind in discovered}
    for kind in ("oov_counts", "oovs_found"):
        for path in discovered[kind]:
            parsed = parse_oov_count_file(path)
            if parsed:
                oov_counts.update(parsed)
                parser = kind
                source_files.append(str(path))
                selected[kind].append(path)
                break
        if oov_counts:
            break
    if not oov_counts:
        for log_path in discovered["normalize_oov_log"]:
            parser = "normalize_oov_log_regex_fallback"
            source_files.append(str(log_path))
            selected["normalize_oov_log"].append(log_path)
            log_text = log_path.read_text(encoding="utf-8")
            for word, count in re.findall(r"'word': '([^']+)'.*?'count': (\d+)", log_text):
                oov_counts[word.lower()] = max(oov_counts.get(word.lower(), 0), int(count))
            if oov_counts:
                break
    if discovered["utterance_oovs"]:
        selected["utterance_oovs"].append(discovered["utterance_oovs"][0])
    copied = snapshot_oov_artifacts(output_root, selected)

    expected_types, expected_tokens = expected_mfa_oov_counts(output_root)
    actual_types = len(oov_counts)
    actual_tokens = sum(oov_counts.values())
    reason_codes: list[str] = []
    if not source_files:
        reason_codes.append("mfa_oov_artifacts_not_found")
    if expected_types is not None and expected_types != actual_types:
        reason_codes.append("oov_count_mismatch")
    if expected_tokens is not None and expected_tokens != actual_tokens:
        reason_codes.append("oov_count_mismatch")
    summary = {
        "oov_source": parser,
        "oov_types": actual_types,
        "oov_tokens": actual_tokens,
        "expected_oov_types": expected_types,
        "expected_oov_tokens": expected_tokens,
        "source_files_found": source_files,
        "snapshot_files": copied,
        "reason_codes": sorted(set(reason_codes)),
    }
    write_json(output_root / "oov_summary.json", summary)
    if expected_types and not oov_counts:
        raise RuntimeError("MFA reported OOVs but no OOV artifact could be parsed")

    rows = [
        {
            "word": word,
            "count": count,
            "is_numeric": bool(re.fullmatch(r"\d+(?:[.,]\d+)?", word)),
        }
        for word, count in sorted(oov_counts.items())
    ]
    write_json(existing_path, rows)
    return set(oov_counts)


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    return float(np.percentile(np.asarray(values, dtype=np.float64), quantile))


def run_alignment_qc(output_root: Path) -> dict[str, Any]:
    buffers = {row["buffer_id"]: row for row in load_json(output_root / "asr_mfa_queue.json")}
    normalized = {row["buffer_id"]: row for row in load_json(output_root / "normalized_transcripts.json")}
    transcripts = {row["buffer_id"]: row for row in load_json(output_root / "transcripts.json")}
    words_by_buffer: dict[str, list[dict[str, Any]]] = {buffer_id: [] for buffer_id in buffers}
    for word in load_jsonl(output_root / "aligned_words.jsonl"):
        words_by_buffer.setdefault(word["buffer_id"], []).append(word)

    by_buffer: list[dict[str, Any]] = []
    all_word_durations: list[float] = []
    all_word_gaps: list[float] = []
    for buffer_id, buffer in sorted(buffers.items()):
        words = sorted(words_by_buffer.get(buffer_id, []), key=lambda row: row["source_start_sample"])
        token_count = len(normalized.get(buffer_id, {}).get("tokens", []))
        reason_codes: list[str] = []
        word_durations: list[float] = []
        word_gaps: list[float] = []
        negative_or_zero = 0
        tiny_words = 0
        long_words = 0
        backwards = 0
        outside_buffer = 0
        outside_trusted = 0
        near_trusted_edges = 0
        symbol_hazard_words = 0
        numeric_hazard_words = 0
        token_word_mismatches = 0

        previous_end: int | None = None
        for word in words:
            start = int(word["source_start_sample"])
            end = int(word["source_end_sample"])
            duration_sec = (end - start) / ANALYSIS_SAMPLE_RATE
            if end <= start:
                negative_or_zero += 1
                reason_codes.append("word_non_positive_duration")
            else:
                word_durations.append(duration_sec)
                all_word_durations.append(duration_sec)
            if 0 < duration_sec < ALIGNMENT_TINY_WORD_SEC:
                tiny_words += 1
                reason_codes.append("word_absurdly_short")
            if duration_sec > ALIGNMENT_LONG_WORD_SEC:
                long_words += 1
                reason_codes.append("word_absurdly_long")
            if previous_end is not None:
                gap_sec = (start - previous_end) / ANALYSIS_SAMPLE_RATE
                word_gaps.append(gap_sec)
                all_word_gaps.append(gap_sec)
                if start < previous_end:
                    backwards += 1
                    reason_codes.append("word_order_backwards")
            previous_end = end

            if start < buffer["source_start_sample"] or end > buffer["source_end_sample"]:
                outside_buffer += 1
                reason_codes.append("word_outside_buffer")
            if start < buffer["trusted_start_sample"] or end > buffer["trusted_end_sample"]:
                outside_trusted += 1
                reason_codes.append("word_outside_trusted_chunk")
            edge = TRUSTED_EDGE_WARN_SEC * ANALYSIS_SAMPLE_RATE
            if abs(start - buffer["trusted_start_sample"]) < edge or abs(end - buffer["trusted_end_sample"]) < edge:
                near_trusted_edges += 1
                reason_codes.append("word_near_trusted_edge")
            if word.get("contains_danger_symbol"):
                symbol_hazard_words += 1
                reason_codes.append("word_contains_symbol_hazard")
            if word.get("contains_numeric"):
                numeric_hazard_words += 1
                reason_codes.append("word_contains_numeric_hazard")
            if word.get("alignment_token_word_mismatch"):
                token_word_mismatches += 1
                reason_codes.append("alignment_token_word_mismatch")

        missing_extra = token_count - len(words)
        if not words:
            reason_codes.append("no_aligned_words")
        if missing_extra != 0:
            reason_codes.append("alignment_token_count_mismatch")

        segments = transcripts.get(buffer_id, {}).get("segments", [])
        by_buffer.append(
            {
                "buffer_id": buffer_id,
                "duration_sec": buffer["duration_sec"],
                "split_strategy": buffer["split_strategy"],
                "normalized_token_count": token_count,
                "aligned_word_count": len(words),
                "missing_extra_word_count": missing_extra,
                "negative_or_zero_word_durations": negative_or_zero,
                "absurdly_short_words": tiny_words,
                "absurdly_long_words": long_words,
                "backwards_word_order": backwards,
                "words_outside_buffer": outside_buffer,
                "words_outside_trusted_chunk": outside_trusted,
                "words_near_trusted_edges": near_trusted_edges,
                "words_with_symbol_hazards": symbol_hazard_words,
                "words_with_numeric_hazards": numeric_hazard_words,
                "alignment_token_word_mismatches": token_word_mismatches,
                "first_word_local_start_sample": None
                if not words
                else int(words[0]["source_start_sample"]) - int(buffer["source_start_sample"]),
                "last_word_local_end_sample": None
                if not words
                else int(words[-1]["source_end_sample"]) - int(buffer["source_start_sample"]),
                "p50_word_duration_sec": percentile(word_durations, 50),
                "p90_word_duration_sec": percentile(word_durations, 90),
                "p50_word_gap_sec": percentile(word_gaps, 50),
                "p90_word_gap_sec": percentile(word_gaps, 90),
                "asr_min_avg_logprob": None
                if not segments
                else min(float(segment["avg_logprob"]) for segment in segments),
                "asr_max_no_speech_prob": None
                if not segments
                else max(float(segment["no_speech_prob"]) for segment in segments),
                "asr_max_compression_ratio": None
                if not segments
                else max(float(segment["compression_ratio"]) for segment in segments),
                "reason_codes": sorted(set(reason_codes)),
            }
        )

    summary = {
        "buffer_count": len(buffers),
        "aligned_word_count": sum(len(words) for words in words_by_buffer.values()),
        "normalized_token_count": sum(len(row.get("tokens", [])) for row in normalized.values()),
        "buffers_with_no_words": sum(row["aligned_word_count"] == 0 for row in by_buffer),
        "buffers_with_alignment_mismatch": sum(row["missing_extra_word_count"] != 0 for row in by_buffer),
        "buffers_with_words_outside_buffer": sum(row["words_outside_buffer"] > 0 for row in by_buffer),
        "buffers_with_words_outside_trusted_chunk": sum(
            row["words_outside_trusted_chunk"] > 0 for row in by_buffer
        ),
        "buffers_with_non_positive_word_durations": sum(
            row["negative_or_zero_word_durations"] > 0 for row in by_buffer
        ),
        "buffers_with_absurdly_short_words": sum(row["absurdly_short_words"] > 0 for row in by_buffer),
        "buffers_with_absurdly_long_words": sum(row["absurdly_long_words"] > 0 for row in by_buffer),
        "buffers_with_backwards_word_order": sum(row["backwards_word_order"] > 0 for row in by_buffer),
        "buffers_with_symbol_hazards": sum(row["words_with_symbol_hazards"] > 0 for row in by_buffer),
        "buffers_with_numeric_hazards": sum(row["words_with_numeric_hazards"] > 0 for row in by_buffer),
        "buffers_with_alignment_token_word_mismatch": sum(
            row["alignment_token_word_mismatches"] > 0 for row in by_buffer
        ),
        "p50_word_duration_sec": percentile(all_word_durations, 50),
        "p90_word_duration_sec": percentile(all_word_durations, 90),
        "p50_word_gap_sec": percentile(all_word_gaps, 50),
        "p90_word_gap_sec": percentile(all_word_gaps, 90),
        "thresholds": {
            "absurdly_short_word_sec": ALIGNMENT_TINY_WORD_SEC,
            "absurdly_long_word_sec": ALIGNMENT_LONG_WORD_SEC,
            "trusted_edge_warn_sec": TRUSTED_EDGE_WARN_SEC,
        },
    }
    write_json(output_root / "alignment_qc_by_buffer.json", by_buffer)
    write_json(output_root / "alignment_qc_summary.json", summary)
    return summary


def dbfs(rms: np.ndarray | float) -> np.ndarray | float:
    return 20.0 * np.log10(np.maximum(rms, 1e-10))


def frame_rms_db(samples: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    frame = sec_to_sample(CUTPOINT_FRAME_SEC)
    hop = sec_to_sample(CUTPOINT_HOP_SEC)
    if len(samples) < frame:
        return np.asarray([], dtype=np.int64), np.asarray([], dtype=np.float32)
    starts = np.arange(0, len(samples) - frame + 1, hop, dtype=np.int64)
    rms_values = np.empty(len(starts), dtype=np.float32)
    for index, start in enumerate(starts):
        window = samples[start : start + frame]
        rms_values[index] = float(np.sqrt(np.mean(np.square(window), dtype=np.float64)))
    db_values = dbfs(rms_values).astype(np.float32)
    if len(db_values) >= 3:
        db_values = np.convolve(db_values, np.ones(3, dtype=np.float32) / 3.0, mode="same")
    centers = starts + frame // 2
    return centers, db_values


def generate_safe_cutpoint_diagnostics(output_root: Path) -> dict[str, Any]:
    buffers = {row["buffer_id"]: row for row in load_json(output_root / "asr_mfa_queue.json")}
    normalized_by_id = {
        row["buffer_id"]: row for row in load_json(output_root / "normalized_transcripts.json")
    }
    words_by_buffer: dict[str, list[dict[str, Any]]] = {buffer_id: [] for buffer_id in buffers}
    for word in load_jsonl(output_root / "aligned_words.jsonl"):
        words_by_buffer.setdefault(word["buffer_id"], []).append(word)

    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    valley_dbs: list[float] = []
    gap_durations: list[float] = []

    left_guard = sec_to_sample(CUTPOINT_LEFT_WORD_EDGE_GUARD_SEC)
    right_guard = sec_to_sample(CUTPOINT_RIGHT_WORD_EDGE_GUARD_SEC)
    oov_guard = sec_to_sample(OOV_CUT_GUARD_SEC)
    symbol_guard = sec_to_sample(SYMBOL_CUT_GUARD_SEC)
    numeric_guard = sec_to_sample(NUMERIC_CUT_GUARD_SEC)
    provisional_guard = sec_to_sample(PROVISIONAL_SPLIT_GUARD_SEC)
    min_gap = sec_to_sample(CUTPOINT_MIN_GAP_SEC)
    for buffer_id, buffer in sorted(buffers.items()):
        audio_path = output_root / buffer.get("queue_audio_path", buffer["audio_path"])
        samples, sample_rate = sf.read(str(audio_path), dtype="float32", always_2d=False)
        if sample_rate != ANALYSIS_SAMPLE_RATE:
            raise ValueError(f"{audio_path} has {sample_rate} Hz, expected {ANALYSIS_SAMPLE_RATE}")
        if isinstance(samples, np.ndarray) and samples.ndim != 1:
            raise ValueError(f"{audio_path} is not mono")

        frame_centers, buffer_db = frame_rms_db(np.asarray(samples, dtype=np.float32))
        noise_floor_db = percentile(buffer_db.tolist(), 10) if len(buffer_db) else None
        words = sorted(words_by_buffer.get(buffer_id, []), key=lambda row: row["source_start_sample"])
        local_words = [
            {
                **word,
                "local_start_sample": int(word["source_start_sample"]) - int(buffer["source_start_sample"]),
                "local_end_sample": int(word["source_end_sample"]) - int(buffer["source_start_sample"]),
            }
            for word in words
        ]
        trusted_words = [
            word
            for word in local_words
            if word["local_start_sample"] >= buffer["trusted_local_start_sample"]
            and word["local_end_sample"] <= buffer["trusted_local_end_sample"]
            and word["local_end_sample"] > word["local_start_sample"]
        ]

        for index, (left, right) in enumerate(zip(trusted_words, trusted_words[1:])):
            gap_start = int(left["local_end_sample"])
            gap_end = int(right["local_start_sample"])
            candidate_start = gap_start + left_guard
            candidate_end = gap_end - right_guard
            usable_gap = candidate_end - candidate_start
            gap_duration_sec = max(0, gap_end - gap_start) / ANALYSIS_SAMPLE_RATE
            reason_codes: list[str] = []
            normalized = normalized_by_id.get(buffer_id, {})
            if normalized.get("disable_automatic_cutpoints"):
                reason_codes.append("buffer_automatic_cutpoints_disabled")
            if buffer.get("left_provisional_boundary") and candidate_start < int(buffer["trusted_local_start_sample"]) + provisional_guard:
                reason_codes.append("near_left_provisional_pre_mfa_split")
            if buffer.get("right_provisional_boundary") and candidate_end > int(buffer["trusted_local_end_sample"]) - provisional_guard:
                reason_codes.append("near_right_provisional_pre_mfa_split")
            if left.get("alignment_token_word_mismatch") or right.get("alignment_token_word_mismatch"):
                reason_codes.append("alignment_token_word_mismatch")
            if left.get("is_oov") or right.get("is_oov"):
                reason_codes.append("adjacent_to_oov")
            if left.get("contains_danger_symbol") or right.get("contains_danger_symbol"):
                reason_codes.append("adjacent_to_symbol_hazard")
            if left.get("contains_numeric") or right.get("contains_numeric"):
                reason_codes.append("adjacent_to_numeric_hazard")
            if any(
                word.get("contains_danger_symbol")
                and candidate_end >= word["local_start_sample"] - symbol_guard
                and candidate_start <= word["local_end_sample"] + symbol_guard
                for word in trusted_words
            ):
                reason_codes.append("near_symbol_hazard")
            if any(
                word.get("contains_numeric")
                and candidate_end >= word["local_start_sample"] - numeric_guard
                and candidate_start <= word["local_end_sample"] + numeric_guard
                for word in trusted_words
            ):
                reason_codes.append("near_numeric_hazard")
            if gap_end <= gap_start:
                reason_codes.append("non_positive_word_gap")
            if usable_gap < min_gap:
                reason_codes.append("usable_gap_too_short")
            if noise_floor_db is None:
                reason_codes.append("missing_buffer_energy")

            valley_sample = None
            valley_db = None
            if not reason_codes:
                mask = (frame_centers >= candidate_start) & (frame_centers <= candidate_end)
                if not np.any(mask):
                    reason_codes.append("no_rms_frame_inside_gap")
                else:
                    scoped_centers = frame_centers[mask]
                    scoped_db = buffer_db[mask]
                    valley_index = int(np.argmin(scoped_db))
                    valley_sample = int(scoped_centers[valley_index])
                    valley_db = float(scoped_db[valley_index])
                    valley_dbs.append(valley_db)
                    gap_durations.append(gap_duration_sec)
                    if any(
                        word.get("is_oov")
                        and valley_sample >= word["local_start_sample"] - oov_guard
                        and valley_sample <= word["local_end_sample"] + oov_guard
                        for word in trusted_words
                    ):
                        reason_codes.append("near_oov_word")
                    if valley_db > float(noise_floor_db) + CUTPOINT_NOISE_MARGIN_DB:
                        reason_codes.append("valley_above_noise_floor_margin")

            row = {
                "id": f"{buffer_id}-cut-{index:04d}",
                "buffer_id": buffer_id,
                "left_word_id": left["id"],
                "right_word_id": right["id"],
                "left_word": left["word"],
                "right_word": right["word"],
                "left_word_is_oov": bool(left.get("is_oov")),
                "right_word_is_oov": bool(right.get("is_oov")),
                "left_word_contains_symbol_hazard": bool(left.get("contains_danger_symbol")),
                "right_word_contains_symbol_hazard": bool(right.get("contains_danger_symbol")),
                "left_word_contains_numeric_hazard": bool(left.get("contains_numeric")),
                "right_word_contains_numeric_hazard": bool(right.get("contains_numeric")),
                "gap_start_local_sample": gap_start,
                "gap_end_local_sample": gap_end,
                "candidate_start_local_sample": candidate_start,
                "candidate_end_local_sample": candidate_end,
                "cut_local_sample": valley_sample,
                "source_sample": None
                if valley_sample is None
                else int(buffer["source_start_sample"]) + int(valley_sample),
                "gap_duration_sec": gap_duration_sec,
                "usable_gap_sec": max(0, usable_gap) / ANALYSIS_SAMPLE_RATE,
                "valley_dbfs": valley_db,
                "noise_floor_dbfs": noise_floor_db,
                "noise_margin_db": None if valley_db is None or noise_floor_db is None else valley_db - float(noise_floor_db),
                "frame_sec": CUTPOINT_FRAME_SEC,
                "hop_sec": CUTPOINT_HOP_SEC,
                "left_word_edge_guard_sec": CUTPOINT_LEFT_WORD_EDGE_GUARD_SEC,
                "right_word_edge_guard_sec": CUTPOINT_RIGHT_WORD_EDGE_GUARD_SEC,
                "reason_codes": sorted(set(reason_codes)),
            }
            if reason_codes:
                rejected.append(row)
            else:
                accepted.append(row)

    write_jsonl(output_root / "safe_cutpoints.jsonl", accepted)
    write_jsonl(output_root / "rejected_cutpoint_candidates.jsonl", rejected)
    summary = {
        "buffers_evaluated": len(buffers),
        "accepted_cutpoints": len(accepted),
        "rejected_cutpoint_candidates": len(rejected),
        "acceptance_rate": 0.0 if not accepted and not rejected else len(accepted) / (len(accepted) + len(rejected)),
        "p50_gap_duration_sec": percentile(gap_durations, 50),
        "p90_gap_duration_sec": percentile(gap_durations, 90),
        "p50_valley_dbfs": percentile(valley_dbs, 50),
        "p90_valley_dbfs": percentile(valley_dbs, 90),
        "rejection_reason_counts": {
            reason: sum(reason in row["reason_codes"] for row in rejected)
            for reason in sorted({reason for row in rejected for reason in row["reason_codes"]})
        },
        "thresholds": {
            "left_word_edge_guard_sec": CUTPOINT_LEFT_WORD_EDGE_GUARD_SEC,
            "right_word_edge_guard_sec": CUTPOINT_RIGHT_WORD_EDGE_GUARD_SEC,
            "min_gap_sec": CUTPOINT_MIN_GAP_SEC,
            "frame_sec": CUTPOINT_FRAME_SEC,
            "hop_sec": CUTPOINT_HOP_SEC,
            "noise_margin_db": CUTPOINT_NOISE_MARGIN_DB,
            "oov_cut_guard_sec": OOV_CUT_GUARD_SEC,
            "symbol_cut_guard_sec": SYMBOL_CUT_GUARD_SEC,
            "numeric_cut_guard_sec": NUMERIC_CUT_GUARD_SEC,
            "provisional_split_guard_sec": PROVISIONAL_SPLIT_GUARD_SEC,
        },
    }
    write_json(output_root / "safe_cutpoint_summary.json", summary)
    return summary


def assemble_candidate_clips_dry_run(output_root: Path) -> dict[str, Any]:
    buffers = {row["buffer_id"]: row for row in load_json(output_root / "asr_mfa_queue.json")}
    words_by_buffer: dict[str, list[dict[str, Any]]] = {buffer_id: [] for buffer_id in buffers}
    cutpoints_by_buffer: dict[str, list[dict[str, Any]]] = {buffer_id: [] for buffer_id in buffers}
    for word in load_jsonl(output_root / "aligned_words.jsonl"):
        words_by_buffer.setdefault(word["buffer_id"], []).append(word)
    for cutpoint in load_jsonl(output_root / "safe_cutpoints.jsonl"):
        cutpoints_by_buffer.setdefault(cutpoint["buffer_id"], []).append(cutpoint)

    min_samples = sec_to_sample(MIN_CANDIDATE_CLIP_SEC)
    target_samples = sec_to_sample(TARGET_CANDIDATE_CLIP_SEC)
    max_samples = sec_to_sample(MAX_CANDIDATE_CLIP_SEC)
    clips: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for buffer_id, buffer in sorted(buffers.items()):
        words = sorted(words_by_buffer.get(buffer_id, []), key=lambda row: row["source_start_sample"])
        cutpoints = sorted(cutpoints_by_buffer.get(buffer_id, []), key=lambda row: row["source_sample"])
        if not words:
            rejected.append(
                {
                    "buffer_id": buffer_id,
                    "reason_codes": [
                        "mfa_textgrid_missing",
                        "no_aligned_words",
                        "buffer_excluded_from_clip_assembly",
                    ],
                }
            )
            continue
        if len(cutpoints) < 2:
            rejected.append(
                {
                    "buffer_id": buffer_id,
                    "reason_codes": ["insufficient_safe_cutpoints", "buffer_excluded_from_clip_assembly"],
                }
            )
            continue
        start_index = 0
        while start_index < len(cutpoints) - 1:
            start = cutpoints[start_index]
            candidates = [
                end
                for end in cutpoints[start_index + 1 :]
                if min_samples <= end["source_sample"] - start["source_sample"] <= max_samples
            ]
            if not candidates:
                rejected.append(
                    {
                        "buffer_id": buffer_id,
                        "start_cutpoint_ref": start["id"],
                        "reason_codes": ["no_valid_end_cutpoint"],
                    }
                )
                start_index += 1
                continue
            end = min(candidates, key=lambda row: abs((row["source_sample"] - start["source_sample"]) - target_samples))
            included_words = [
                word
                for word in words
                if word["source_start_sample"] >= start["source_sample"]
                and word["source_end_sample"] <= end["source_sample"]
            ]
            review_reasons = sorted(
                {
                    reason
                    for word in included_words
                    for reason in word.get("review_reason_codes", [])
                }
                | (
                    {"clip_contains_symbol_hazard", "transcript_requires_review"}
                    if any(word.get("contains_danger_symbol") for word in included_words)
                    else set()
                )
                | (
                    {"clip_contains_numeric_token", "transcript_requires_review"}
                    if any(word.get("contains_numeric") for word in included_words)
                    else set()
                )
            )
            raw_tokens: list[str] = []
            previous_raw_token_id = None
            for word in included_words:
                raw_token_id = word.get("raw_token_id")
                if raw_token_id and raw_token_id == previous_raw_token_id:
                    continue
                raw_tokens.append(word.get("raw_token") or word["word"])
                previous_raw_token_id = raw_token_id
            clips.append(
                {
                    "id": f"candidate_clip_{len(clips):06d}",
                    "buffer_id": buffer_id,
                    "start_cutpoint_ref": start["id"],
                    "end_cutpoint_ref": end["id"],
                    "source_start_sample": start["source_sample"],
                    "source_end_sample": end["source_sample"],
                    "duration_samples": end["source_sample"] - start["source_sample"],
                    "duration_sec": (end["source_sample"] - start["source_sample"]) / ANALYSIS_SAMPLE_RATE,
                    "word_ids": [word["id"] for word in included_words],
                    "training_text": " ".join(raw_tokens),
                    "alignment_text": " ".join(word["word"] for word in included_words),
                    "needs_review": bool(review_reasons),
                    "review_reason_codes": review_reasons,
                    "dry_run_only": True,
                }
            )
            start_index = cutpoints.index(end)
    write_json(output_root / "candidate_clips_dry_run.json", clips)
    write_json(output_root / "candidate_clips_dry_run_rejected.json", rejected)
    summary = {
        "candidate_clips": len(clips),
        "rejected_spans": len(rejected),
        "clips_needing_review": sum(row["needs_review"] for row in clips),
        "clips_needing_review_for_symbols": sum(
            "clip_contains_symbol_hazard" in row["review_reason_codes"] for row in clips
        ),
        "clips_needing_review_for_numbers": sum(
            "clip_contains_numeric_token" in row["review_reason_codes"] for row in clips
        ),
        "min_clip_duration_sec": min((row["duration_sec"] for row in clips), default=None),
        "max_clip_duration_sec": max((row["duration_sec"] for row in clips), default=None),
        "dry_run_only": True,
    }
    write_json(output_root / "candidate_clips_dry_run_summary.json", summary)
    return summary


def extract_candidate_review_clips(output_root: Path) -> dict[str, Any]:
    analysis_path = output_root / f"{SUBSET_BASENAME}_mono16k.wav"
    audio, sample_rate = sf.read(str(analysis_path), dtype="float32", always_2d=False)
    if sample_rate != ANALYSIS_SAMPLE_RATE or not isinstance(audio, np.ndarray) or audio.ndim != 1:
        raise ValueError("Final clip source must be mono 16 kHz analysis audio")
    clips = load_json(output_root / "candidate_clips_dry_run.json")
    review_dir = output_root / "candidate_review_clips"
    if review_dir.exists():
        shutil.rmtree(review_dir)
    ensure_dir(review_dir)
    manifest: list[dict[str, Any]] = []
    for clip in clips:
        start = int(clip["source_start_sample"])
        end = int(clip["source_end_sample"])
        if start < 0 or end > len(audio) or end <= start:
            raise RuntimeError(f"Invalid final clip bounds: {clip['id']}")
        audio_rel_path = Path("candidate_review_clips") / f"{clip['id']}.wav"
        sf.write(str(output_root / audio_rel_path), audio[start:end], sample_rate, subtype="PCM_16")
        manifest.append(
            {
                **clip,
                "audio_path": str(audio_rel_path),
                "dry_run_only": False,
            }
        )
    write_json(output_root / "candidate_review_manifest.json", manifest)
    summary = {
        "candidate_review_clips": len(manifest),
        "total_duration_sec": sum(row["duration_sec"] for row in manifest),
        "clips_needing_review": sum(row["needs_review"] for row in manifest),
        "output_dir": str(review_dir),
    }
    write_json(output_root / "candidate_review_summary.json", summary)
    return summary


def run_asr_mfa_pipeline(output_root: Path) -> None:
    asr_summary = run_asr_for_queue(output_root)
    normalization_summary = normalize_transcripts(output_root)
    corpus_summary = prepare_mfa_corpus(output_root)
    mfa_summary = run_mfa_alignment(output_root)
    alignment_qc_summary = run_alignment_qc(output_root) if (output_root / "aligned_words.jsonl").exists() else {}
    print("asr_summary:", asr_summary)
    print("normalization_summary:", normalization_summary)
    print("mfa_corpus_summary:", {k: v for k, v in corpus_summary.items() if k != "skipped"})
    print("mfa_summary:", {k: v for k, v in mfa_summary.items() if not k.endswith("_tail")})
    print("alignment_qc_summary:", alignment_qc_summary)



def main() -> None:
    ensure_dir(PROJECT_ROOT / "scripts")
    ensure_dir(OUTPUT_ROOT)

    subset_path = OUTPUT_ROOT / f"{SUBSET_BASENAME}.wav"
    analysis_path = OUTPUT_ROOT / f"{SUBSET_BASENAME}_mono16k.wav"
    subset_meta_path = OUTPUT_ROOT / "subset_metadata.json"
    analysis_meta_path = OUTPUT_ROOT / "analysis_audio_metadata.json"
    vad_path = OUTPUT_ROOT / "vad_segments.jsonl"
    vad_summary_path = OUTPUT_ROOT / "vad_summary.json"
    diarization_summary_path = OUTPUT_ROOT / "diarization_summary.json"
    speaker_regions_path = OUTPUT_ROOT / "speaker_regions.jsonl"

    subset_metadata = extract_subset(SOURCE_AUDIO, subset_path)
    analysis_metadata = create_analysis_audio(subset_path, analysis_path)
    audio_tensor, sample_rate = load_analysis_audio(analysis_path)
    vad_segments, vad_summary = run_silero(audio_tensor, sample_rate)
    diarization_result = run_diarization_or_fallback(analysis_path, vad_segments)

    write_json(subset_meta_path, subset_metadata)
    write_json(analysis_meta_path, analysis_metadata)
    write_jsonl(vad_path, [asdict(segment) for segment in vad_segments])
    write_json(vad_summary_path, vad_summary)
    write_json(diarization_summary_path, {k: v for k, v in diarization_result.items() if k != "speaker_regions"})
    write_jsonl(speaker_regions_path, diarization_result["speaker_regions"])
    buffer_summary = build_processing_buffers(analysis_path, vad_path, speaker_regions_path, OUTPUT_ROOT)

    print("subset_audio:", subset_path)
    print("analysis_audio:", analysis_path)
    print("vad_segments:", vad_path)
    print("speaker_regions:", speaker_regions_path)
    print("diarization_status:", diarization_result["status"])
    print("processing_buffers:", buffer_summary["processing_buffers"])


if __name__ == "__main__":
    main()
