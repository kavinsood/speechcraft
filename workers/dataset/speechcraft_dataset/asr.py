from __future__ import annotations

import gc
import shutil
from pathlib import Path
from typing import Any

from .io import read_json_value, resolve_under_root, sha256_file, write_json
from .models import TimeoutError, resolve_asr_model_reference, timeout_after


def build_asr_queue(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    buffers_path = resolve_under_root(run_root, "artifacts/processing_buffers.json")
    buffers = list(read_json_value(buffers_path))
    min_buffer_sec = float(config.get("min_asr_mfa_buffer_sec", 5.0))
    queue: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    queue_dir = resolve_under_root(run_root, "artifacts/asr_mfa_queue")
    if queue_dir.exists():
        shutil.rmtree(queue_dir)
    queue_dir.mkdir(parents=True, exist_ok=True)
    for buffer in buffers:
        row = dict(buffer)
        if float(row["duration_sec"]) < min_buffer_sec:
            row["queue_status"] = "rejected"
            row["queue_reason_codes"] = ["buffer_under_min_asr_mfa_sec"]
            rejected.append(row)
            continue
        source_path = resolve_under_root(run_root, str(row["audio_path"]))
        queue_rel_path = f"artifacts/asr_mfa_queue/{Path(str(row['audio_path'])).name}"
        queue_path = resolve_under_root(run_root, queue_rel_path)
        shutil.copy2(source_path, queue_path)
        row["queue_status"] = "asr_mfa_ready"
        row["queue_reason_codes"] = ["provisional_pre_mfa_split_review"] if "forced_chunk_split" in row["reason_codes"] else []
        row["queue_audio_path"] = queue_rel_path
        row["queue_audio_hash"] = sha256_file(queue_path)
        queue.append(row)
    queue_path = resolve_under_root(run_root, "artifacts/asr_mfa_queue.json")
    rejected_path = resolve_under_root(run_root, "artifacts/rejected_buffers.json")
    write_json(queue_path, queue)
    write_json(rejected_path, rejected)
    summary = {
        "stage": "asr_queue",
        "config_hash": str(config.get("config_hash") or ""),
        "input_artifact_hashes": {
            "processing_buffers_json": sha256_file(buffers_path),
        },
        "output_hashes": {
            "asr_mfa_queue_json": sha256_file(queue_path),
            "rejected_buffers_json": sha256_file(rejected_path),
        },
        "ready_buffers": len(queue),
        "rejected_buffers": len(rejected),
        "ready_duration_sec": round(sum(float(row["duration_sec"]) for row in queue), 6),
        "min_ready_buffer_sec": min_buffer_sec,
    }
    write_json(resolve_under_root(run_root, "artifacts/asr_mfa_queue_summary.json"), summary)
    return summary


def run_asr(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    queue_path = resolve_under_root(run_root, "artifacts/asr_mfa_queue.json")
    queue = list(read_json_value(queue_path))
    transcripts_path = resolve_under_root(run_root, "artifacts/transcripts.json")
    if not queue:
        write_json(transcripts_path, [])
        summary = {
            "stage": "asr",
            "config_hash": str(config.get("config_hash") or ""),
            "input_artifact_hashes": {
                "asr_mfa_queue_json": sha256_file(queue_path),
            },
            "output_hashes": {
                "transcripts_json": sha256_file(transcripts_path),
            },
            "backend": "faster-whisper",
            "model": str(config.get("faster_whisper_model") or "small.en"),
            "device": str(config.get("faster_whisper_device") or ""),
            "compute_type": str(config.get("faster_whisper_compute_type") or ""),
            "buffer_count": 0,
            "empty_transcripts": 0,
            "total_chars": 0,
            "reason_codes": ["empty_asr_queue"],
        }
        write_json(resolve_under_root(run_root, "artifacts/transcripts_summary.json"), summary)
        return summary

    try:
        import torch
        from faster_whisper import WhisperModel
    except Exception as exc:
        raise RuntimeError(f"ASR dependencies are unavailable: {type(exc).__name__}: {exc}") from exc

    model_name = str(config.get("faster_whisper_model") or "small.en")
    model_reference = resolve_asr_model_reference(config)
    device = str(config.get("faster_whisper_device") or ("cuda" if torch.cuda.is_available() else "cpu"))
    compute_type = str(config.get("faster_whisper_compute_type") or ("float16" if device == "cuda" else "int8"))
    beam_size = int(config.get("faster_whisper_beam_size", 5))
    model_load_timeout = int(config.get("asr_model_load_timeout_sec") or 180)
    transcribe_timeout = int(config.get("asr_transcribe_timeout_sec") or 600)
    language_setting = config.get("asr_language")
    if language_setting in {None, "", "auto"}:
        language = None
    else:
        language = str(language_setting)
    task = str(config.get("asr_task") or "transcribe")
    vad_filter = bool(config.get("asr_vad_filter", False))
    word_timestamps = bool(config.get("asr_word_timestamps", False))
    condition_on_previous_text = bool(config.get("asr_condition_on_previous_text", False))
    transcripts: list[dict[str, Any]] = []
    model = None

    try:
        try:
            with timeout_after(model_load_timeout, "ASR model load"):
                model = WhisperModel(model_reference, device=device, compute_type=compute_type)
        except TimeoutError as exc:
            raise RuntimeError(
                f"ASR model load timed out: requested={model_name!r}, "
                f"resolved={model_reference!r}, device={device!r}, compute_type={compute_type!r}: {exc}"
            ) from exc
        except Exception as exc:
            raise RuntimeError(
                f"ASR model unavailable: requested={model_name!r}, "
                f"resolved={model_reference!r}, device={device!r}, compute_type={compute_type!r}: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        for buffer in queue:
            audio_path = resolve_under_root(run_root, str(buffer.get("queue_audio_path") or buffer["audio_path"]))
            try:
                with timeout_after(transcribe_timeout, f"ASR transcription for {buffer['buffer_id']}"):
                    segments, info = model.transcribe(
                        str(audio_path),
                        language=language,
                        task=task,
                        vad_filter=vad_filter,
                        word_timestamps=word_timestamps,
                        condition_on_previous_text=condition_on_previous_text,
                        beam_size=beam_size,
                    )
            except TimeoutError as exc:
                raise RuntimeError(f"ASR transcription timed out for {buffer['buffer_id']}: {exc}") from exc
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
                    "audio_path": str(buffer.get("queue_audio_path") or buffer["audio_path"]),
                    "text": text,
                    "segments": segment_rows,
                    "language": info.language,
                    "language_probability": info.language_probability,
                    "duration": info.duration,
                    "duration_after_vad": getattr(info, "duration_after_vad", None),
                    "asr_backend": "faster-whisper",
                    "asr_model": model_name,
                    "asr_model_reference": model_reference,
                    "asr_language": language,
                    "asr_task": task,
                    "asr_vad_filter": vad_filter,
                    "asr_word_timestamps": word_timestamps,
                    "asr_condition_on_previous_text": condition_on_previous_text,
                    "device": device,
                    "compute_type": compute_type,
                    "reason_codes": [] if text else ["empty_asr_transcript"],
                }
            )
    finally:
        if model is not None:
            del model
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

    write_json(transcripts_path, transcripts)
    summary = {
        "stage": "asr",
        "config_hash": str(config.get("config_hash") or ""),
        "input_artifact_hashes": {
            "asr_mfa_queue_json": sha256_file(queue_path),
        },
        "output_hashes": {
            "transcripts_json": sha256_file(transcripts_path),
        },
        "backend": "faster-whisper",
        "model": model_name,
        "model_reference": model_reference,
        "device": device,
        "compute_type": compute_type,
        "language": language,
        "task": task,
        "vad_filter": vad_filter,
        "word_timestamps": word_timestamps,
        "condition_on_previous_text": condition_on_previous_text,
        "buffer_count": len(transcripts),
        "empty_transcripts": sum(not row["text"] for row in transcripts),
        "total_chars": sum(len(row["text"]) for row in transcripts),
    }
    write_json(resolve_under_root(run_root, "artifacts/transcripts_summary.json"), summary)
    return summary
