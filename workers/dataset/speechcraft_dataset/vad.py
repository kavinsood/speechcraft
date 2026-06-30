from __future__ import annotations

import gc
from pathlib import Path
from typing import Any

from .io import read_json, resolve_under_root, sha256_file, write_json, write_jsonl


def run_silero_vad(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    try:
        import numpy as np
        import soundfile as sf
        import torch
        from silero_vad import get_speech_timestamps, load_silero_vad
    except Exception as exc:
        raise RuntimeError(f"Silero VAD dependencies are unavailable: {type(exc).__name__}: {exc}") from exc

    audio_variants_manifest_path = resolve_under_root(run_root, "artifacts/audio_variants_manifest.json")
    manifest = read_json(audio_variants_manifest_path)
    variants = list(manifest.get("variants") or [])
    model = load_silero_vad()
    threshold = float(config.get("vad_threshold", 0.5))
    min_speech_ms = int(config.get("vad_min_speech_ms", 250))
    min_silence_ms = int(config.get("vad_min_silence_ms", 250))
    speech_pad_ms = int(config.get("vad_speech_pad_ms", 80))

    try:
        rows: list[dict[str, Any]] = []
        per_source: dict[str, dict[str, Any]] = {}
        for variant in variants:
            source_audio_id = str(variant["source_audio_id"])
            sample_rate = int(variant["sample_rate"])
            audio_path = resolve_under_root(run_root, str(variant["path"]))
            samples, actual_sample_rate = sf.read(str(audio_path), dtype="float32", always_2d=False)
            if int(actual_sample_rate) != sample_rate:
                raise ValueError(f"Analysis sample-rate mismatch for {source_audio_id}: {actual_sample_rate} != {sample_rate}")
            if isinstance(samples, np.ndarray) and samples.ndim != 1:
                raise ValueError(f"Analysis audio must be mono for VAD: {source_audio_id}")
            audio_tensor = torch.from_numpy(np.asarray(samples, dtype=np.float32))
            timestamps = get_speech_timestamps(
                audio_tensor,
                model,
                sampling_rate=sample_rate,
                threshold=threshold,
                min_speech_duration_ms=min_speech_ms,
                min_silence_duration_ms=min_silence_ms,
                speech_pad_ms=speech_pad_ms,
                return_seconds=False,
            )
            speech_samples = 0
            for index, timestamp in enumerate(timestamps):
                start_sample = int(timestamp["start"])
                end_sample = int(timestamp["end"])
                speech_samples += max(0, end_sample - start_sample)
                rows.append(
                    {
                        "id": f"{source_audio_id}_vad_{index:06d}",
                        "source_audio_id": source_audio_id,
                        "analysis_audio_path": variant["path"],
                        "analysis_start_sample": start_sample,
                        "analysis_end_sample": end_sample,
                        "start_sample": start_sample,
                        "end_sample": end_sample,
                        "analysis_start_sec": round(start_sample / sample_rate, 6),
                        "analysis_end_sec": round(end_sample / sample_rate, 6),
                        "start_sec": round(start_sample / sample_rate, 6),
                        "end_sec": round(end_sample / sample_rate, 6),
                        "backend": "silero_vad",
                        "backend_version": getattr(model, "__class__", type(model)).__name__,
                        "threshold": threshold,
                    }
                )
            per_source[source_audio_id] = {
                "segment_count": len(timestamps),
                "speech_duration_sec": round(speech_samples / sample_rate, 6),
                "speech_ratio": round(speech_samples / max(1, int(variant["num_samples"])), 6),
            }

        summary = {
            "stage": "vad",
            "config_hash": str(config.get("config_hash") or ""),
            "input_artifact_hashes": {
                "audio_variants_manifest": sha256_file(audio_variants_manifest_path),
            },
            "backend": "silero_vad",
            "segment_count": len(rows),
            "source_count": len(variants),
            "threshold": threshold,
            "min_speech_ms": min_speech_ms,
            "min_silence_ms": min_silence_ms,
            "speech_pad_ms": speech_pad_ms,
            "per_source": per_source,
        }
        vad_segments_path = resolve_under_root(run_root, "artifacts/vad_segments.jsonl")
        write_jsonl(vad_segments_path, rows)
        summary["output_hashes"] = {
            "vad_segments_jsonl": sha256_file(vad_segments_path),
        }
        write_json(resolve_under_root(run_root, "artifacts/vad_summary.json"), summary)
        return summary
    finally:
        try:
            del model
            del audio_tensor
        except UnboundLocalError:
            pass
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
