from __future__ import annotations

import gc
import json
import shutil
from collections import Counter
from pathlib import Path
from typing import Any

from .io import read_json, read_jsonl, resolve_under_root, run_command, sha256_file, write_json, write_jsonl


def sec_to_sample(seconds: float, sample_rate: int) -> int:
    return int(round(seconds * sample_rate))


def sample_to_sec(sample_index: int, sample_rate: int) -> float:
    return round(sample_index / sample_rate, 6)


def window_ranges(duration_sec: float, window_sec: float, overlap_sec: float) -> list[tuple[float, float]]:
    if duration_sec <= window_sec:
        return [(0.0, duration_sec)]
    step = window_sec - overlap_sec
    if step <= 0:
        raise ValueError("diarization_window_sec must be larger than diarization_window_overlap_sec")
    rows: list[tuple[float, float]] = []
    cursor = 0.0
    while cursor < duration_sec:
        end = min(duration_sec, cursor + window_sec)
        rows.append((cursor, end))
        if end >= duration_sec:
            break
        cursor += step
    return rows


def extract_audio_window(source_path: Path, output_path: Path, start_sec: float, end_sec: float, sample_rate: int) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    run_command(
        [
            "ffmpeg",
            "-y",
            "-ss",
            f"{start_sec:.3f}",
            "-i",
            str(source_path),
            "-t",
            f"{max(0.0, end_sec - start_sec):.3f}",
            "-ac",
            "1",
            "-ar",
            str(sample_rate),
            "-c:a",
            "pcm_s16le",
            str(output_path),
        ]
    )


def local_vad_segments_for_window(vad_segments: list[dict[str, Any]], start_sec: float, end_sec: float, sample_rate: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for segment in vad_segments:
        clipped_start = max(float(segment["analysis_start_sec"]), start_sec)
        clipped_end = min(float(segment["analysis_end_sec"]), end_sec)
        if clipped_end <= clipped_start:
            continue
        local_start_sample = sec_to_sample(clipped_start - start_sec, sample_rate)
        local_end_sample = sec_to_sample(clipped_end - start_sec, sample_rate)
        rows.append(
            {
                "id": str(segment["id"]),
                "start_sec": sample_to_sec(local_start_sample, sample_rate),
                "end_sec": sample_to_sec(local_end_sample, sample_rate),
                "start_sample": local_start_sample,
                "end_sample": local_end_sample,
            }
        )
    return rows


def write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True) + "\n", encoding="utf-8")


def write_external_vad_manifest(path: Path, audio_path: Path, vad_segments: list[dict[str, Any]], uniq_id: str) -> None:
    rows = [
        {
            "audio_filepath": str(audio_path),
            "offset": round(float(segment["start_sec"]), 5),
            "duration": round(float(segment["end_sec"]) - float(segment["start_sec"]), 5),
            "label": "UNK",
            "uniq_id": uniq_id,
        }
        for segment in vad_segments
        if int(segment["end_sample"]) > int(segment["start_sample"])
    ]
    write_jsonl(path, rows)


def parse_rttm_line(line: str, sample_rate: int, backend_version: str | None) -> dict[str, Any]:
    parts = line.strip().split()
    if len(parts) < 8:
        raise ValueError(f"Invalid RTTM line: {line!r}")
    start_sec = float(parts[3])
    duration_sec = float(parts[4])
    end_sec = start_sec + duration_sec
    local_label = parts[7]
    start_sample = sec_to_sample(start_sec, sample_rate)
    end_sample = sec_to_sample(end_sec, sample_rate)
    return {
        "window_local_speaker_label": local_label,
        "start_sec": start_sec,
        "end_sec": end_sec,
        "start_sample": start_sample,
        "end_sample": end_sample,
        "backend": "nemo_clustering_diarizer",
        "backend_version": backend_version,
        "vad_source": "silero_external",
        "rfc_compliant": True,
        "source": "pred_rttm",
    }


def temporal_overlap(a: dict[str, Any], b: dict[str, Any], start_sec: float, end_sec: float) -> float:
    start = max(float(a["start_sec"]), float(b["start_sec"]), start_sec)
    end = min(float(a["end_sec"]), float(b["end_sec"]), end_sec)
    return max(0.0, end - start)


def load_window_regions(summary: dict[str, Any], backend_version: str | None, sample_rate: int) -> list[dict[str, Any]]:
    pred_rttm_path = Path(str(summary["pred_rttm"]))
    window_start_sec = float(summary["start_sec"])
    rows: list[dict[str, Any]] = []
    for line in pred_rttm_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        region = parse_rttm_line(line, sample_rate, backend_version)
        region["window_id"] = str(summary["id"])
        region["start_sec"] += window_start_sec
        region["end_sec"] += window_start_sec
        region["start_sample"] = sec_to_sample(float(region["start_sec"]), sample_rate)
        region["end_sample"] = sec_to_sample(float(region["end_sec"]), sample_rate)
        region["source"] = str(pred_rttm_path)
        rows.append(region)
    return rows


def stitch_window_speaker_labels(window_summaries: list[dict[str, Any]], backend_version: str | None, sample_rate: int, overlap_sec: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    global_index = 0
    previous_regions: list[dict[str, Any]] = []
    previous_map: dict[str, str] = {}
    kept_rows: list[dict[str, Any]] = []
    window_label_mappings: dict[str, dict[str, str]] = {}

    for window_index, summary in enumerate(window_summaries):
        if summary.get("status") != "ok":
            continue
        regions = load_window_regions(summary, backend_version, sample_rate)
        local_labels = sorted({str(row["window_local_speaker_label"]) for row in regions})
        local_to_global: dict[str, str] = {}

        if window_index > 0 and previous_regions:
            overlap_start = float(summary["start_sec"])
            overlap_end = overlap_start + overlap_sec
            scores: dict[tuple[str, str], float] = {}
            for current in regions:
                current_label = str(current["window_local_speaker_label"])
                for previous in previous_regions:
                    previous_label = str(previous["window_local_speaker_label"])
                    overlap = temporal_overlap(previous, current, overlap_start, overlap_end)
                    if overlap > 0:
                        scores[(current_label, previous_label)] = scores.get((current_label, previous_label), 0.0) + overlap

            used_previous: set[str] = set()
            for current_label in local_labels:
                candidates = [
                    (score, previous_label)
                    for (score_label, previous_label), score in scores.items()
                    if score_label == current_label and previous_label not in used_previous
                ]
                if candidates:
                    score, previous_label = max(candidates)
                    if score >= 1.0 and previous_label in previous_map:
                        local_to_global[current_label] = previous_map[previous_label]
                        used_previous.add(previous_label)

        for local_label in local_labels:
            if local_label not in local_to_global:
                local_to_global[local_label] = f"speaker_{global_index}"
                global_index += 1

        keep_start = float(summary["start_sec"]) if window_index == 0 else float(summary["start_sec"]) + (overlap_sec / 2.0)
        keep_end = float(summary["end_sec"])
        for region in regions:
            if float(region["end_sec"]) <= keep_start or float(region["start_sec"]) >= keep_end:
                continue
            start_sec = max(float(region["start_sec"]), keep_start)
            end_sec = min(float(region["end_sec"]), keep_end)
            start_sample = sec_to_sample(start_sec, sample_rate)
            end_sample = sec_to_sample(end_sec, sample_rate)
            speaker_id = local_to_global[str(region["window_local_speaker_label"])]
            kept_rows.append(
                {
                    **region,
                    "speaker_id": speaker_id,
                    "start_sec": start_sec,
                    "end_sec": end_sec,
                    "start_sample": start_sample,
                    "end_sample": end_sample,
                    "id": f"{speaker_id}-{start_sample}-{end_sample}",
                }
            )

        window_label_mappings[str(summary["id"])] = local_to_global
        previous_regions = regions
        previous_map = local_to_global

    return kept_rows, {
        "method": "adjacent_window_temporal_overlap",
        "min_overlap_sec": 1.0,
        "window_label_mappings": window_label_mappings,
    }


def run_nemo_window(
    *,
    analysis_path: Path,
    source_audio_id: str,
    window_dir: Path,
    window_index: int,
    start_sec: float,
    end_sec: float,
    sample_rate: int,
    vad_segments: list[dict[str, Any]],
    backend_version: str | None,
    diarization_device: str,
    speaker_model: str,
    max_speakers: int,
    batch_size: int,
    save_embeddings: bool,
) -> dict[str, Any]:
    from nemo.collections.asr.models import ClusteringDiarizer
    from nemo.collections.asr.models.configs.diarizer_config import NeuralDiarizerInferenceConfig

    window_id = f"{source_audio_id}_window_{window_index:03d}"
    if window_dir.exists():
        shutil.rmtree(window_dir)
    window_dir.mkdir(parents=True, exist_ok=True)

    local_vad_segments = local_vad_segments_for_window(vad_segments, start_sec, end_sec, sample_rate)
    if not local_vad_segments:
        return {
            "id": window_id,
            "source_audio_id": source_audio_id,
            "start_sec": start_sec,
            "end_sec": end_sec,
            "status": "skipped_no_vad",
            "speaker_regions": 0,
            "vad_segment_count": 0,
        }

    window_audio_path = window_dir / f"{window_id}.wav"
    input_manifest_path = window_dir / "input_manifest.json"
    external_vad_manifest_path = window_dir / "external_vad_manifest.json"
    extract_audio_window(analysis_path, window_audio_path, start_sec, end_sec, sample_rate)
    write_manifest(
        input_manifest_path,
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
    write_external_vad_manifest(external_vad_manifest_path, window_audio_path, local_vad_segments, window_id)

    cfg = NeuralDiarizerInferenceConfig()
    cfg.device = diarization_device
    cfg.verbose = False
    cfg.batch_size = batch_size
    cfg.num_workers = 0
    cfg.sample_rate = sample_rate
    cfg.diarizer.manifest_filepath = str(input_manifest_path)
    cfg.diarizer.out_dir = str(window_dir)
    cfg.diarizer.oracle_vad = False
    cfg.diarizer.vad.model_path = None
    cfg.diarizer.vad.external_vad_manifest = str(external_vad_manifest_path)
    cfg.diarizer.speaker_embeddings.model_path = speaker_model
    cfg.diarizer.speaker_embeddings.parameters.window_length_in_sec = (1.5,)
    cfg.diarizer.speaker_embeddings.parameters.shift_length_in_sec = (0.75,)
    cfg.diarizer.speaker_embeddings.parameters.multiscale_weights = (1,)
    cfg.diarizer.speaker_embeddings.parameters.save_embeddings = save_embeddings
    cfg.diarizer.clustering.parameters.oracle_num_speakers = False
    cfg.diarizer.clustering.parameters.max_num_speakers = max_speakers

    diarizer = None
    try:
        diarizer = ClusteringDiarizer(cfg=cfg)
        diarizer.diarize()
    finally:
        if diarizer is not None:
            del diarizer
        gc.collect()
        try:
            import torch

            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        except Exception:
            pass

    pred_rttm_path = window_dir / "pred_rttms" / f"{window_id}.rttm"
    if not pred_rttm_path.exists():
        raise FileNotFoundError(f"Expected NeMo RTTM output at {pred_rttm_path}")

    return {
        "id": window_id,
        "source_audio_id": source_audio_id,
        "start_sec": start_sec,
        "end_sec": end_sec,
        "status": "ok",
        "speaker_regions": 0,
        "pred_rttm": str(pred_rttm_path),
        "external_vad_manifest": str(external_vad_manifest_path),
        "manifest": str(input_manifest_path),
        "vad_segment_count": len(local_vad_segments),
        "backend_version": backend_version,
    }


def build_single_speaker_regions(variants: list[dict[str, Any]], vad_segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    variant_paths = {str(variant["source_audio_id"]): str(variant["path"]) for variant in variants}
    rows: list[dict[str, Any]] = []
    for segment in vad_segments:
        source_audio_id = str(segment["source_audio_id"])
        start_sample = int(segment["analysis_start_sample"])
        end_sample = int(segment["analysis_end_sample"])
        rows.append(
            {
                "id": f"speaker_0-{source_audio_id}-{start_sample}-{end_sample}",
                "source_audio_id": source_audio_id,
                "analysis_audio_path": variant_paths[source_audio_id],
                "speaker_id": "speaker_0",
                "start_sample": start_sample,
                "end_sample": end_sample,
                "start_sec": float(segment["analysis_start_sec"]),
                "end_sec": float(segment["analysis_end_sec"]),
                "backend": "single_speaker_vad_passthrough",
                "backend_version": None,
                "vad_source": "silero_external",
                "rfc_compliant": True,
            }
        )
    return rows


def write_speaker_samples(
    run_root: Path,
    rows: list[dict[str, Any]],
    *,
    sample_rate_by_source: dict[str, int],
    analysis_path_by_source: dict[str, str],
    sample_count: int,
    sample_duration_sec: float,
) -> list[dict[str, Any]]:
    by_speaker: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_speaker.setdefault(str(row["speaker_id"]), []).append(row)

    manifest_rows: list[dict[str, Any]] = []
    for speaker_id, speaker_rows in sorted(by_speaker.items()):
        ranked = sorted(speaker_rows, key=lambda row: int(row["end_sample"]) - int(row["start_sample"]), reverse=True)[:sample_count]
        for index, row in enumerate(ranked):
            source_audio_id = str(row["source_audio_id"])
            sample_rate = sample_rate_by_source[source_audio_id]
            duration_samples = min(int(row["end_sample"]) - int(row["start_sample"]), sec_to_sample(sample_duration_sec, sample_rate))
            if duration_samples <= 0:
                continue
            start_sample = int(row["start_sample"])
            end_sample = start_sample + duration_samples
            analysis_path = resolve_under_root(run_root, analysis_path_by_source[source_audio_id])
            relative_audio_path = f"artifacts/speaker_samples/{speaker_id}_{index:02d}.wav"
            output_path = resolve_under_root(run_root, relative_audio_path)
            extract_audio_window(
                analysis_path,
                output_path,
                start_sample / sample_rate,
                end_sample / sample_rate,
                sample_rate,
            )
            manifest_rows.append(
                {
                    "sample_id": f"{speaker_id}_{index:02d}",
                    "speaker_id": speaker_id,
                    "source_audio_id": source_audio_id,
                    "region_id": str(row["id"]),
                    "audio_path": relative_audio_path,
                    "start_sample": start_sample,
                    "end_sample": end_sample,
                    "duration_sec": sample_to_sec(duration_samples, sample_rate),
                    "content_hash": sha256_file(output_path),
                }
            )
    return manifest_rows


def build_summary(rows: list[dict[str, Any]], sample_rows: list[dict[str, Any]], *, stage: str, config: dict[str, Any], input_hashes: dict[str, str], backend: str, backend_version: str | None, mode: str, reason_codes: list[str] | None = None, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    per_speaker: dict[str, dict[str, Any]] = {}
    for row in rows:
        speaker_id = str(row["speaker_id"])
        speaker = per_speaker.setdefault(speaker_id, {"segment_count": 0, "duration_sec": 0.0, "source_audio_ids": set()})
        speaker["segment_count"] += 1
        speaker["duration_sec"] += max(0.0, float(row["end_sec"]) - float(row["start_sec"]))
        speaker["source_audio_ids"].add(str(row["source_audio_id"]))
    for speaker in per_speaker.values():
        speaker["duration_sec"] = round(float(speaker["duration_sec"]), 6)
        speaker["source_audio_ids"] = sorted(speaker["source_audio_ids"])

    summary = {
        "stage": stage,
        "config_hash": str(config.get("config_hash") or ""),
        "input_artifact_hashes": input_hashes,
        "backend": backend,
        "backend_version": backend_version,
        "mode": mode,
        "speaker_count": len(per_speaker),
        "speaker_ids": sorted(per_speaker),
        "speaker_sample_count": len(sample_rows),
        "per_speaker": per_speaker,
        "reason_codes": reason_codes or [],
    }
    if extra:
        summary.update(extra)
    return summary


def plan_concat_layout(
    variants: list[dict[str, Any]], gap_sec: float
) -> list[dict[str, Any]]:
    """Cumulative concat offsets (seconds) for each source variant, separated by
    a silence gap so no speech region spans a file boundary. Pure/testable."""
    layout: list[dict[str, Any]] = []
    cursor = 0.0
    for variant in variants:
        duration = float(variant["analysis_duration_sec"])
        start = cursor
        end = start + duration
        layout.append(
            {
                "source_audio_id": str(variant["source_audio_id"]),
                "path": str(variant["path"]),
                "start_sec": round(start, 6),
                "end_sec": round(end, 6),
                "duration_sec": round(duration, 6),
            }
        )
        cursor = end + gap_sec
    return layout


def offset_vad_for_concat(
    vad_segments: list[dict[str, Any]], layout: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Shift each source's VAD segments into concat time. Pure/testable."""
    start_by_source = {row["source_audio_id"]: float(row["start_sec"]) for row in layout}
    rows: list[dict[str, Any]] = []
    for segment in vad_segments:
        source_audio_id = str(segment["source_audio_id"])
        offset = start_by_source.get(source_audio_id)
        if offset is None:
            continue
        rows.append(
            {
                "id": str(segment["id"]),
                "source_audio_id": "concat",
                "analysis_start_sec": float(segment["analysis_start_sec"]) + offset,
                "analysis_end_sec": float(segment["analysis_end_sec"]) + offset,
            }
        )
    return rows


def remap_concat_regions_to_sources(
    regions: list[dict[str, Any]], layout: list[dict[str, Any]], sample_rate: int
) -> list[dict[str, Any]]:
    """Map concat-time speaker regions back to per-source local coordinates,
    dropping anything that falls in an inter-file gap. Pure/testable."""
    rows: list[dict[str, Any]] = []
    for region in regions:
        r_start = float(region["start_sec"])
        r_end = float(region["end_sec"])
        mid = (r_start + r_end) / 2.0
        entry = next(
            (e for e in layout if float(e["start_sec"]) <= mid < float(e["end_sec"])),
            None,
        )
        if entry is None:
            continue  # region sits in the silence gap between files
        offset = float(entry["start_sec"])
        local_start = max(r_start, float(entry["start_sec"])) - offset
        local_end = min(r_end, float(entry["end_sec"])) - offset
        if local_end <= local_start:
            continue
        start_sample = sec_to_sample(local_start, sample_rate)
        end_sample = sec_to_sample(local_end, sample_rate)
        speaker_id = str(region["speaker_id"])
        source_audio_id = str(entry["source_audio_id"])
        rows.append(
            {
                **region,
                "speaker_id": speaker_id,
                "source_audio_id": source_audio_id,
                "analysis_audio_path": str(entry["path"]),
                "start_sec": sample_to_sec(start_sample, sample_rate),
                "end_sec": sample_to_sec(end_sample, sample_rate),
                "start_sample": start_sample,
                "end_sample": end_sample,
                "id": f"{speaker_id}-{source_audio_id}-{start_sample}-{end_sample}",
            }
        )
    return rows


def build_concat_analysis_wav(
    run_root: Path,
    layout: list[dict[str, Any]],
    *,
    sample_rate: int,
    gap_sec: float,
) -> Path:
    """Concatenate the per-source analysis variants (uniform mono @ sample_rate)
    into one WAV with silence gaps between sources, via the ffmpeg concat demuxer."""
    concat_dir = resolve_under_root(run_root, "artifacts/diarization/_concat")
    if concat_dir.exists():
        shutil.rmtree(concat_dir)
    concat_dir.mkdir(parents=True, exist_ok=True)

    silence_path = concat_dir / "silence.wav"
    run_command(
        [
            "ffmpeg", "-y", "-f", "lavfi",
            "-i", f"anullsrc=r={sample_rate}:cl=mono",
            "-t", f"{gap_sec:.3f}", "-c:a", "pcm_s16le", str(silence_path),
        ]
    )

    list_lines: list[str] = []
    for index, entry in enumerate(layout):
        abs_path = resolve_under_root(run_root, str(entry["path"])).resolve()
        list_lines.append(f"file '{abs_path}'")
        if index < len(layout) - 1:
            list_lines.append(f"file '{silence_path.resolve()}'")
    list_path = concat_dir / "concat_list.txt"
    list_path.write_text("\n".join(list_lines) + "\n", encoding="utf-8")

    concat_path = concat_dir / "concat_analysis.wav"
    run_command(
        [
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", str(list_path), "-ac", "1", "-ar", str(sample_rate),
            "-c:a", "pcm_s16le", str(concat_path),
        ]
    )
    return concat_path


def _run_multi_file_diarization(
    run_root: Path,
    config: dict[str, Any],
    variants: list[dict[str, Any]],
    vad_segments: list[dict[str, Any]],
    *,
    sample_rate_by_source: dict[str, int],
    analysis_path_by_source: dict[str, str],
    sample_count: int,
    sample_duration_sec: float,
) -> dict[str, Any]:
    """Diarize multiple sources on one concatenated timeline so speaker identity
    is consistent across files, then remap regions back per source, and write
    the same samples/selection/summary artifacts as the single-file path."""
    import nemo  # noqa: F401
    import torch

    sample_rate = int(variants[0]["analysis_sample_rate"])
    gap_sec = float(config.get("diarization_concat_gap_sec") or 1.0)
    layout = plan_concat_layout(variants, gap_sec)
    concat_path = build_concat_analysis_wav(run_root, layout, sample_rate=sample_rate, gap_sec=gap_sec)
    concat_vad = offset_vad_for_concat(vad_segments, layout)
    concat_duration = float(layout[-1]["end_sec"]) if layout else 0.0

    window_sec = float(config.get("diarization_window_sec") or 900.0)
    overlap_sec = float(config.get("diarization_window_overlap_sec") or 30.0)
    diarization_device = str(config.get("diarization_device") or ("cuda" if torch.cuda.is_available() else "cpu"))
    speaker_model = str(config.get("diarization_speaker_model") or "titanet_large")
    max_speakers = int(config.get("diarization_max_speakers") or 6)
    batch_size = int(config.get("diarization_batch_size") or 16)
    save_embeddings = bool(config.get("diarization_save_embeddings", False))

    diarization_dir = resolve_under_root(run_root, "artifacts/diarization/concat")
    if diarization_dir.exists():
        shutil.rmtree(diarization_dir)
    diarization_dir.mkdir(parents=True, exist_ok=True)

    window_summaries: list[dict[str, Any]] = []
    for window_index, (start_sec, end_sec) in enumerate(window_ranges(concat_duration, window_sec, overlap_sec)):
        window_summaries.append(
            run_nemo_window(
                analysis_path=concat_path,
                source_audio_id="concat",
                window_dir=diarization_dir / f"window_{window_index:03d}",
                window_index=window_index,
                start_sec=start_sec,
                end_sec=end_sec,
                sample_rate=sample_rate,
                vad_segments=concat_vad,
                backend_version=getattr(nemo, "__version__", None),
                diarization_device=diarization_device,
                speaker_model=speaker_model,
                max_speakers=max_speakers,
                batch_size=batch_size,
                save_embeddings=save_embeddings,
            )
        )

    concat_rows, _stitching = stitch_window_speaker_labels(
        window_summaries, getattr(nemo, "__version__", None), sample_rate, overlap_sec
    )
    rows = remap_concat_regions_to_sources(concat_rows, layout, sample_rate)

    speaker_regions_path = resolve_under_root(run_root, "artifacts/speaker_regions.jsonl")
    samples_manifest_path = resolve_under_root(run_root, "artifacts/speaker_samples_manifest.json")
    selection_path = resolve_under_root(run_root, "artifacts/speaker_selection.json")
    audio_variants_manifest_path = resolve_under_root(run_root, "artifacts/audio_variants_manifest.json")
    vad_segments_path = resolve_under_root(run_root, "artifacts/vad_segments.jsonl")

    sample_rows = write_speaker_samples(
        run_root,
        rows,
        sample_rate_by_source=sample_rate_by_source,
        analysis_path_by_source=analysis_path_by_source,
        sample_count=sample_count,
        sample_duration_sec=sample_duration_sec,
    )
    write_jsonl(speaker_regions_path, rows)
    write_json(samples_manifest_path, sample_rows)

    available_speaker_ids = sorted({str(row["speaker_id"]) for row in rows})
    selection: dict[str, Any] = {
        "mode": "diarization",
        "selected": False,
        "target_speaker_id": None,
        "source": "pending_user_selection",
        "available_speaker_ids": available_speaker_ids,
        "updated_at": None,
    }
    if selection_path.exists():
        existing = read_json(selection_path)
        existing_target = str(existing.get("target_speaker_id") or "").strip()
        if existing_target in available_speaker_ids:
            selection = {
                "mode": "diarization",
                "selected": True,
                "target_speaker_id": existing_target,
                "source": str(existing.get("source") or "user"),
                "available_speaker_ids": available_speaker_ids,
                "updated_at": existing.get("updated_at"),
            }
    write_json(selection_path, selection)

    summary = build_summary(
        rows,
        sample_rows,
        stage="diarization",
        config=config,
        input_hashes={
            "audio_variants_manifest": sha256_file(audio_variants_manifest_path),
            "vad_segments_jsonl": sha256_file(vad_segments_path),
        },
        backend="nemo_clustering_diarizer_concat_multifile",
        backend_version=getattr(nemo, "__version__", None),
        mode=str(config.get("mode") or "diarization"),
        reason_codes=[] if selection["selected"] else ["speaker_selection_required"],
        extra={
            "selection_written": True,
            "multi_file": True,
            "source_count": len(variants),
            "source_audio_ids": [str(v["source_audio_id"]) for v in variants],
            "concat_gap_sec": gap_sec,
        },
    )
    summary["output_hashes"] = {
        "speaker_regions_jsonl": sha256_file(speaker_regions_path),
        "speaker_samples_manifest_json": sha256_file(samples_manifest_path),
        "speaker_selection_json": sha256_file(selection_path),
    }
    write_json(resolve_under_root(run_root, "artifacts/speaker_regions_summary.json"), summary)
    return summary


def run_diarization(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    audio_variants_manifest_path = resolve_under_root(run_root, "artifacts/audio_variants_manifest.json")
    vad_segments_path = resolve_under_root(run_root, "artifacts/vad_segments.jsonl")
    speaker_regions_path = resolve_under_root(run_root, "artifacts/speaker_regions.jsonl")
    samples_manifest_path = resolve_under_root(run_root, "artifacts/speaker_samples_manifest.json")
    selection_path = resolve_under_root(run_root, "artifacts/speaker_selection.json")
    variants = list(read_json(audio_variants_manifest_path).get("variants") or [])
    vad_segments = read_jsonl(vad_segments_path)

    sample_rate_by_source = {str(variant["source_audio_id"]): int(variant["analysis_sample_rate"]) for variant in variants}
    analysis_path_by_source = {str(variant["source_audio_id"]): str(variant["path"]) for variant in variants}
    mode = str(config.get("mode") or "single_speaker")
    sample_count = int(config.get("speaker_sample_count") or 3)
    sample_duration_sec = float(config.get("speaker_sample_duration_sec") or 6.0)

    if mode == "single_speaker":
        rows = build_single_speaker_regions(variants, vad_segments)
        sample_rows = write_speaker_samples(
            run_root,
            rows,
            sample_rate_by_source=sample_rate_by_source,
            analysis_path_by_source=analysis_path_by_source,
            sample_count=sample_count,
            sample_duration_sec=sample_duration_sec,
        )
        write_jsonl(speaker_regions_path, rows)
        write_json(samples_manifest_path, sample_rows)
        selection = {
            "mode": "single_speaker",
            "selected": True,
            "target_speaker_id": "speaker_0",
            "source": "auto",
            "available_speaker_ids": ["speaker_0"],
            "updated_at": None,
        }
        write_json(selection_path, selection)
        summary = build_summary(
            rows,
            sample_rows,
            stage="diarization",
            config=config,
            input_hashes={
                "audio_variants_manifest": sha256_file(audio_variants_manifest_path),
                "vad_segments_jsonl": sha256_file(vad_segments_path),
            },
            backend="single_speaker_vad_passthrough",
            backend_version=None,
            mode=mode,
            extra={"selection_written": True},
        )
        summary["output_hashes"] = {
            "speaker_regions_jsonl": sha256_file(speaker_regions_path),
            "speaker_samples_manifest_json": sha256_file(samples_manifest_path),
            "speaker_selection_json": sha256_file(selection_path),
        }
        write_json(resolve_under_root(run_root, "artifacts/speaker_regions_summary.json"), summary)
        return summary

    if len(variants) > 1:
        return _run_multi_file_diarization(
            run_root,
            config,
            variants,
            vad_segments,
            sample_rate_by_source=sample_rate_by_source,
            analysis_path_by_source=analysis_path_by_source,
            sample_count=sample_count,
            sample_duration_sec=sample_duration_sec,
        )

    try:
        import nemo
        import torch
    except Exception as exc:
        raise RuntimeError(f"NeMo diarization dependencies are unavailable: {type(exc).__name__}: {exc}") from exc

    variant = variants[0]
    source_audio_id = str(variant["source_audio_id"])
    analysis_path = resolve_under_root(run_root, str(variant["path"]))
    sample_rate = int(variant["analysis_sample_rate"])
    duration_sec = float(variant["analysis_duration_sec"])
    source_vad_segments = [row for row in vad_segments if str(row["source_audio_id"]) == source_audio_id]
    diarization_dir = resolve_under_root(run_root, f"artifacts/diarization/{source_audio_id}")
    if diarization_dir.exists():
        shutil.rmtree(diarization_dir)
    diarization_dir.mkdir(parents=True, exist_ok=True)

    window_sec = float(config.get("diarization_window_sec") or 900.0)
    overlap_sec = float(config.get("diarization_window_overlap_sec") or 30.0)
    diarization_device = str(config.get("diarization_device") or ("cuda" if torch.cuda.is_available() else "cpu"))
    speaker_model = str(config.get("diarization_speaker_model") or "titanet_large")
    max_speakers = int(config.get("diarization_max_speakers") or 6)
    batch_size = int(config.get("diarization_batch_size") or 16)
    save_embeddings = bool(config.get("diarization_save_embeddings", False))

    window_summaries: list[dict[str, Any]] = []
    for window_index, (start_sec, end_sec) in enumerate(window_ranges(duration_sec, window_sec, overlap_sec)):
        summary = run_nemo_window(
            analysis_path=analysis_path,
            source_audio_id=source_audio_id,
            window_dir=diarization_dir / f"window_{window_index:03d}",
            window_index=window_index,
            start_sec=start_sec,
            end_sec=end_sec,
            sample_rate=sample_rate,
            vad_segments=source_vad_segments,
            backend_version=getattr(nemo, "__version__", None),
            diarization_device=diarization_device,
            speaker_model=speaker_model,
            max_speakers=max_speakers,
            batch_size=batch_size,
            save_embeddings=save_embeddings,
        )
        window_summaries.append(summary)

    rows, stitching_summary = stitch_window_speaker_labels(window_summaries, getattr(nemo, "__version__", None), sample_rate, overlap_sec)
    for row in rows:
        row["source_audio_id"] = source_audio_id
        row["analysis_audio_path"] = str(variant["path"])

    sample_rows = write_speaker_samples(
        run_root,
        rows,
        sample_rate_by_source=sample_rate_by_source,
        analysis_path_by_source=analysis_path_by_source,
        sample_count=sample_count,
        sample_duration_sec=sample_duration_sec,
    )
    write_jsonl(speaker_regions_path, rows)
    write_json(samples_manifest_path, sample_rows)

    available_speaker_ids = sorted({str(row["speaker_id"]) for row in rows})
    selection = {
        "mode": "diarization",
        "selected": False,
        "target_speaker_id": None,
        "source": "pending_user_selection",
        "available_speaker_ids": available_speaker_ids,
        "updated_at": None,
    }
    if selection_path.exists():
        existing = read_json(selection_path)
        existing_target = str(existing.get("target_speaker_id") or "").strip()
        if existing_target in available_speaker_ids:
            selection = {
                "mode": "diarization",
                "selected": True,
                "target_speaker_id": existing_target,
                "source": str(existing.get("source") or "user"),
                "available_speaker_ids": available_speaker_ids,
                "updated_at": existing.get("updated_at"),
            }
    write_json(selection_path, selection)

    summary = build_summary(
        rows,
        sample_rows,
        stage="diarization",
        config=config,
        input_hashes={
            "audio_variants_manifest": sha256_file(audio_variants_manifest_path),
            "vad_segments_jsonl": sha256_file(vad_segments_path),
        },
        backend="nemo_clustering_diarizer_windowed",
        backend_version=getattr(nemo, "__version__", None),
        mode=mode,
        reason_codes=[] if selection["selected"] else ["speaker_selection_required"],
        extra={
            "selection_written": True,
            "source_audio_id": source_audio_id,
            "window_sec": window_sec,
            "window_overlap_sec": overlap_sec,
            "device": diarization_device,
            "speaker_model": speaker_model,
            "batch_size": batch_size,
            "max_speakers": max_speakers,
            "window_count": len(window_summaries),
            "window_status_counts": dict(Counter(str(row.get("status") or "unknown") for row in window_summaries)),
            "speaker_label_stitching": {
                "method": stitching_summary["method"],
                "min_overlap_sec": stitching_summary["min_overlap_sec"],
                "window_count": len(stitching_summary["window_label_mappings"]),
            },
        },
    )
    summary["output_hashes"] = {
        "speaker_regions_jsonl": sha256_file(speaker_regions_path),
        "speaker_samples_manifest_json": sha256_file(samples_manifest_path),
        "speaker_selection_json": sha256_file(selection_path),
    }
    write_json(resolve_under_root(run_root, "artifacts/speaker_regions_summary.json"), summary)
    return summary
