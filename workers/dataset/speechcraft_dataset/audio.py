from __future__ import annotations

import wave
from pathlib import Path
from typing import Any

from .io import resolve_under_root, run_command, sha256_file, write_json


def inspect_wav(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Source WAV not found: {path}")
    if path.suffix.lower() != ".wav":
        raise ValueError(f"Only WAV inputs are supported right now: {path}")

    with wave.open(str(path), "rb") as handle:
        num_channels = handle.getnchannels()
        sample_width = handle.getsampwidth()
        sample_rate = handle.getframerate()
        num_samples = handle.getnframes()
        compression = handle.getcomptype()

    if compression != "NONE":
        raise ValueError(f"Compressed WAV is not supported: {path}")
    if sample_rate <= 0:
        raise ValueError(f"Invalid WAV sample rate: {path}")
    if num_channels <= 0:
        raise ValueError(f"Invalid WAV channel count: {path}")

    return {
        "path": str(path.resolve()),
        "filename": path.name,
        "content_hash": sha256_file(path),
        "sample_rate": sample_rate,
        "num_channels": num_channels,
        "sample_width_bytes": sample_width,
        "num_samples": num_samples,
        "duration_sec": round(num_samples / sample_rate, 6),
    }


def create_analysis_audio_variants(run_root: Path, sources: list[dict[str, Any]], analysis_sample_rate: int) -> dict[str, Any]:
    from .channel_resolver import ffmpeg_channel_args, resolve_source_channel

    variants: list[dict[str, Any]] = []
    for source in sources:
        source_audio_id = str(source["source_audio_id"])
        source_path = Path(str(source["path"]))
        relative_path = f"audio/analysis/{source_audio_id}.mono{analysis_sample_rate}.wav"
        output_path = resolve_under_root(run_root, relative_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        resolution = resolve_source_channel(source_path, int(source.get("num_channels") or 1))
        channel_args = ffmpeg_channel_args(resolution.decision) if resolution else ["-ac", "1"]
        channel_mode = resolution.decision if resolution else "mono_average"
        channel_reason_codes = list(resolution.reason_codes) if resolution else []
        run_command(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(source_path),
                *channel_args,
                "-ar",
                str(analysis_sample_rate),
                "-c:a",
                "pcm_s16le",
                str(output_path),
            ]
        )
        analysis = inspect_wav(output_path)
        variants.append(
            {
                "source_audio_id": source_audio_id,
                "source_recording_id": source["source_recording_id"],
                "kind": "analysis_audio",
                "path": relative_path,
                "source_sample_rate": source["sample_rate"],
                "analysis_sample_rate": analysis["sample_rate"],
                "source_num_samples": source["num_samples"],
                "analysis_num_samples": analysis["num_samples"],
                "source_start_sample": 0,
                "analysis_start_sample": 0,
                "source_duration_sec": source["duration_sec"],
                "analysis_duration_sec": analysis["duration_sec"],
                "resample_ratio": analysis["sample_rate"] / source["sample_rate"],
                "sample_rate": analysis["sample_rate"],
                "num_channels": analysis["num_channels"],
                "num_samples": analysis["num_samples"],
                "duration_sec": analysis["duration_sec"],
                "content_hash": analysis["content_hash"],
                "input_artifact_hashes": {
                    "source_audio": source["content_hash"],
                },
                "recipe": {
                    "backend": "ffmpeg",
                    "channel_mode": channel_mode,
                    "channel_reason_codes": channel_reason_codes,
                    "target_sample_rate": analysis_sample_rate,
                    "sample_format": "s16",
                    "codec": "pcm_s16le",
                    "normalization": "none",
                    "clipping_behavior": "ffmpeg_default_s16",
                },
            }
        )

    summary = {
        "variant_count": len(variants),
        "analysis_sample_rate": analysis_sample_rate,
        "channel_mode": "mono_average",
        "codec": "pcm_s16le",
        "total_duration_sec": round(sum(float(variant["duration_sec"]) for variant in variants), 6),
        "all_mono": all(int(variant["num_channels"]) == 1 for variant in variants),
        "variant_hashes": {variant["source_audio_id"]: variant["content_hash"] for variant in variants},
    }
    write_json(resolve_under_root(run_root, "artifacts/audio_variants_manifest.json"), {"variants": variants})
    write_json(resolve_under_root(run_root, "artifacts/audio_variants_summary.json"), summary)
    return summary
