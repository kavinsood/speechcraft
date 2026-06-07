from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import __version__
from .alignment_qc import run_alignment_qc
from .assembly import assemble_candidate_review_clips
from .audio import create_analysis_audio_variants, inspect_wav
from .asr import build_asr_queue, run_asr
from .buffers import run_processing_buffers
from .diarization import run_diarization
from .export import export_native_candidate_clips
from .io import read_json, resolve_under_root, write_json
from .mfa import run_mfa_alignment
from .normalization import normalize_transcripts
from .safecut import generate_safe_cutpoint_diagnostics
from .vad import run_silero_vad


PIPELINE_VERSION = "pretraining_rfc_v1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_config(source_wavs: list[Path], *, single_speaker: bool, target_speaker_label: str) -> dict[str, Any]:
    return {
        "pipeline_version": PIPELINE_VERSION,
        "mode": "single_speaker" if single_speaker else "diarization",
        "source_wavs": [str(path.resolve()) for path in source_wavs],
        "target_speaker_label": target_speaker_label,
        "analysis_sample_rate": 16000,
        "vad_backend": "silero",
        "vad_threshold": 0.5,
        "vad_min_speech_ms": 250,
        "vad_min_silence_ms": 250,
        "vad_speech_pad_ms": 80,
        "diarization_window_sec": 900.0,
        "diarization_window_overlap_sec": 30.0,
        "diarization_max_speakers": 6,
        "diarization_batch_size": 16,
        "diarization_speaker_model": "titanet_large",
        "diarization_save_embeddings": False,
        "speaker_sample_count": 3,
        "speaker_sample_duration_sec": 6.0,
        "max_processing_buffer_sec": 29.5,
        "processing_buffer_pad_sec": 0.5,
        "target_processing_chunk_sec": 24.0,
        "min_split_gap_sec": 0.12,
        "cutpoint_left_word_edge_guard_ms": 30,
        "cutpoint_min_gap_ms": 80,
        "cutpoint_right_word_edge_guard_ms": 30,
        "cutpoint_noise_margin_db": 6.0,
        "cutpoint_frame_ms": 20,
        "cutpoint_hop_ms": 10,
        "oov_cut_guard_sec": 0.5,
        "symbol_cut_guard_sec": 0.5,
        "numeric_cut_guard_sec": 0.5,
        "provisional_split_guard_sec": 0.5,
        "candidate_min_clip_sec": 3.0,
        "candidate_target_clip_sec": 8.0,
        "candidate_max_clip_sec": 15.0,
        "min_asr_mfa_buffer_sec": 5.0,
        "faster_whisper_model": "small.en",
        "faster_whisper_beam_size": 5,
        "asr_model_load_timeout_sec": 180,
        "asr_transcribe_timeout_sec": 600,
        "asr_language": "en",
        "asr_task": "transcribe",
        "asr_vad_filter": False,
        "asr_condition_on_previous_text": False,
        "asr_word_timestamps": False,
        "mfa_dictionary": "english_us_mfa",
        "mfa_acoustic_model": "english_mfa",
        "mfa_single_speaker": True,
        "mfa_timeout_sec": 3600,
        "alignment_tiny_word_sec": 0.020,
        "alignment_long_word_sec": 2.0,
        "alignment_trusted_edge_warn_sec": 0.080,
    }


def load_config(config_path: Path | None, source_wavs: list[Path], *, single_speaker: bool, target_speaker_label: str) -> dict[str, Any]:
    config = default_config(source_wavs, single_speaker=single_speaker, target_speaker_label=target_speaker_label)
    if config_path is not None:
        overrides = read_json(config_path)
        config.update(overrides)
    if not config.get("source_wavs"):
        raise ValueError("At least one source WAV is required")
    config["config_hash"] = config_hash(config)
    return config


def config_hash(config: dict[str, Any]) -> str:
    payload = {key: value for key, value in config.items() if key != "config_hash"}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def log_line(run_root: Path, message: str) -> None:
    log_path = resolve_under_root(run_root, "logs/dataset_worker.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{utc_now_iso()} {message}\n")


def write_status(run_root: Path, payload: dict[str, Any]) -> None:
    write_json(resolve_under_root(run_root, "status.json"), payload)


def runtime_versions() -> dict[str, Any]:
    return {
        "dataset_worker": __version__,
        "python": {
            "executable": sys.executable,
            "version": platform.python_version(),
            "platform": platform.platform(),
        },
    }


def run_prepare_sources(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    source_paths = [Path(raw).expanduser().resolve() for raw in config.get("source_wavs", [])]
    sources = []
    for index, source_path in enumerate(source_paths):
        source = inspect_wav(source_path)
        source["source_audio_id"] = f"source_audio_{index:04d}"
        source["source_recording_id"] = source_path.stem
        sources.append(source)

    total_duration = round(sum(float(source["duration_sec"]) for source in sources), 6)
    manifest = {
        "pipeline_version": PIPELINE_VERSION,
        "source_count": len(sources),
        "sources": sources,
    }
    summary = {
        "source_count": len(sources),
        "total_duration_sec": total_duration,
        "sample_rates": sorted({source["sample_rate"] for source in sources}),
        "channel_counts": sorted({source["num_channels"] for source in sources}),
    }
    write_json(resolve_under_root(run_root, "artifacts/source_audio_manifest.json"), manifest)
    write_json(resolve_under_root(run_root, "artifacts/source_audio_summary.json"), summary)
    return summary


def run_audio_variants(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    source_manifest = read_json(resolve_under_root(run_root, "artifacts/source_audio_manifest.json"))
    sources = list(source_manifest.get("sources") or [])
    return create_analysis_audio_variants(run_root, sources, int(config.get("analysis_sample_rate") or 16000))


def should_stop(current_stage: str, stop_after: str) -> bool:
    order = [
        "source_audio",
        "audio_variants",
        "vad",
        "diarization",
        "buffers",
        "asr_queue",
        "asr",
        "normalization",
        "mfa",
        "alignment_qc",
        "safe_cutpoints",
        "candidate_review_clips",
        "native_export",
    ]
    return order.index(current_stage) >= order.index(stop_after)


def failure_reason_codes(stage: str, exc: Exception) -> list[str]:
    message = str(exc)
    if stage == "vad" and "Silero VAD dependencies are unavailable" in message:
        return ["missing_silero_vad_dependency"]
    if stage == "audio_variants" and "ffmpeg" in message.lower():
        return ["audio_variant_materialization_failed"]
    if stage == "diarization" and "NeMo diarization dependencies are unavailable" in message:
        return ["missing_nemo_dependency"]
    if stage == "diarization":
        return ["diarization_failed"]
    if stage == "buffers":
        return ["processing_buffer_build_failed"]
    if stage == "asr" and "ASR dependencies are unavailable" in message:
        return ["missing_asr_dependency"]
    if stage == "asr" and "ASR model load timed out" in message:
        return ["asr_model_load_timeout"]
    if stage == "asr" and "ASR transcription timed out" in message:
        return ["asr_transcription_timeout"]
    if stage == "asr" and ("Hub" in message or "snapshot" in message or "model" in message.lower()):
        return ["asr_model_unavailable"]
    if stage == "asr":
        return ["asr_failed"]
    if stage == "asr_queue":
        return ["asr_queue_build_failed"]
    if stage == "normalization":
        return ["normalization_failed"]
    if stage == "mfa" and ("MFA binary not configured" in message or "MFA binary path does not exist" in message or "MFA binary not found on PATH" in message):
        return ["missing_mfa_binary"]
    if stage == "mfa" and "mfa_timeout" in message:
        return ["mfa_timeout"]
    if stage == "mfa":
        return ["mfa_failed"]
    if stage == "alignment_qc":
        return ["alignment_qc_failed"]
    if stage == "safe_cutpoints":
        return ["safe_cutpoint_generation_failed"]
    if stage == "candidate_review_clips":
        return ["candidate_review_clip_assembly_failed"]
    if stage == "native_export":
        return ["native_export_failed"]
    return ["dataset_worker_failed"]


def run_dataset_worker(args: argparse.Namespace) -> int:
    run_root = Path(args.run_root).expanduser().resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    status = {
        "ok": None,
        "stage": "starting",
        "reason_codes": [],
        "started_at": utc_now_iso(),
        "completed_at": None,
    }
    write_status(run_root, status)
    try:
        source_wavs = [Path(path) for path in args.source_wav]
        config = load_config(
            Path(args.config).expanduser().resolve() if args.config else None,
            source_wavs,
            single_speaker=args.single_speaker,
            target_speaker_label=args.target_speaker_label,
        )
        write_json(resolve_under_root(run_root, "config.json"), config)
        write_json(resolve_under_root(run_root, "runtime_versions.json"), runtime_versions())
        log_line(run_root, "dataset worker started")
        log_line(run_root, f"mode={config['mode']} source_count={len(config['source_wavs'])}")

        status.update({"stage": "source_audio", "reason_codes": []})
        write_status(run_root, status)
        source_summary = run_prepare_sources(run_root, config)
        log_line(run_root, f"source_audio completed summary={source_summary}")
        if should_stop("source_audio", args.stop_after):
            status.update(
                {
                    "ok": True,
                    "stage": "source_audio",
                    "summary": source_summary,
                    "completed_at": utc_now_iso(),
                }
            )
            write_status(run_root, status)
            return 0

        status.update({"stage": "audio_variants", "summary": source_summary})
        write_status(run_root, status)
        audio_variant_summary = run_audio_variants(run_root, config)
        log_line(run_root, f"audio_variants completed summary={audio_variant_summary}")
        if should_stop("audio_variants", args.stop_after):
            status.update(
                {
                    "ok": True,
                    "stage": "audio_variants",
                    "summary": audio_variant_summary,
                    "completed_at": utc_now_iso(),
                }
            )
            write_status(run_root, status)
            return 0

        status.update({"stage": "vad", "summary": audio_variant_summary})
        write_status(run_root, status)
        vad_backend = str(config.get("vad_backend") or "silero").strip().lower()
        if vad_backend != "silero":
            raise ValueError(f"Unsupported VAD backend: {vad_backend}")
        vad_summary = run_silero_vad(run_root, config)
        log_line(run_root, f"vad completed summary={vad_summary}")
        if should_stop("vad", args.stop_after):
            status.update(
                {
                    "ok": True,
                    "stage": "vad",
                    "summary": vad_summary,
                    "completed_at": utc_now_iso(),
                }
            )
            write_status(run_root, status)
            log_line(run_root, "dataset worker completed VAD")
            return 0

        status.update({"stage": "diarization", "summary": vad_summary})
        write_status(run_root, status)
        diarization_summary = run_diarization(run_root, config)
        log_line(run_root, f"diarization completed summary={diarization_summary}")
        diarization_requires_selection = (
            str(config.get("mode") or "single_speaker") == "diarization"
            and "speaker_selection_required" in list(diarization_summary.get("reason_codes") or [])
            and args.stop_after != "diarization"
        )
        if should_stop("diarization", args.stop_after) or diarization_requires_selection:
            status.update(
                {
                    "ok": True,
                    "stage": "diarization",
                    "summary": diarization_summary,
                    "reason_codes": list(diarization_summary.get("reason_codes") or []),
                    "completed_at": utc_now_iso(),
                }
            )
            write_status(run_root, status)
            log_line(run_root, "dataset worker completed diarization")
            return 0

        status.update({"stage": "buffers", "summary": diarization_summary})
        write_status(run_root, status)
        buffer_summary = run_processing_buffers(run_root, config)
        log_line(run_root, f"buffers completed summary={buffer_summary}")
        if should_stop("buffers", args.stop_after):
            status.update(
                {
                    "ok": True,
                    "stage": "buffers",
                    "summary": buffer_summary,
                    "completed_at": utc_now_iso(),
                }
            )
            write_status(run_root, status)
            log_line(run_root, "dataset worker completed processing buffers")
            return 0

        status.update({"stage": "asr_queue", "summary": buffer_summary})
        write_status(run_root, status)
        asr_queue_summary = build_asr_queue(run_root, config)
        log_line(run_root, f"asr_queue completed summary={asr_queue_summary}")
        if should_stop("asr_queue", args.stop_after):
            status.update(
                {
                    "ok": True,
                    "stage": "asr_queue",
                    "summary": asr_queue_summary,
                    "completed_at": utc_now_iso(),
                }
            )
            write_status(run_root, status)
            return 0

        status.update({"stage": "asr", "summary": asr_queue_summary})
        write_status(run_root, status)
        asr_summary = run_asr(run_root, config)
        log_line(run_root, f"asr completed summary={asr_summary}")
        if should_stop("asr", args.stop_after):
            status.update(
                {
                    "ok": True,
                    "stage": "asr",
                    "summary": asr_summary,
                    "completed_at": utc_now_iso(),
                }
            )
            write_status(run_root, status)
            return 0

        status.update({"stage": "normalization", "summary": asr_summary})
        write_status(run_root, status)
        normalization_summary = normalize_transcripts(run_root, config)
        log_line(run_root, f"normalization completed summary={normalization_summary}")
        if should_stop("normalization", args.stop_after):
            status.update(
                {
                    "ok": True,
                    "stage": "normalization",
                    "summary": normalization_summary,
                    "completed_at": utc_now_iso(),
                }
            )
            write_status(run_root, status)
            log_line(run_root, "dataset worker completed normalization")
            return 0

        status.update({"stage": "mfa", "summary": normalization_summary})
        write_status(run_root, status)
        mfa_summary = run_mfa_alignment(run_root, config)
        log_line(run_root, f"mfa completed summary={mfa_summary}")
        if should_stop("mfa", args.stop_after):
            status.update(
                {
                    "ok": True,
                    "stage": "mfa",
                    "summary": mfa_summary,
                    "completed_at": utc_now_iso(),
                }
            )
            write_status(run_root, status)
            log_line(run_root, "dataset worker completed MFA")
            return 0

        status.update({"stage": "alignment_qc", "summary": mfa_summary})
        write_status(run_root, status)
        alignment_qc_summary = run_alignment_qc(run_root, config)
        log_line(run_root, f"alignment_qc completed summary={alignment_qc_summary}")
        if should_stop("alignment_qc", args.stop_after):
            status.update(
                {
                    "ok": True,
                    "stage": "alignment_qc",
                    "summary": alignment_qc_summary,
                    "completed_at": utc_now_iso(),
                }
            )
            write_status(run_root, status)
            log_line(run_root, "dataset worker completed alignment QC")
            return 0

        status.update({"stage": "safe_cutpoints", "summary": alignment_qc_summary})
        write_status(run_root, status)
        safe_cutpoint_summary = generate_safe_cutpoint_diagnostics(run_root, config)
        log_line(run_root, f"safe_cutpoints completed summary={safe_cutpoint_summary}")
        if should_stop("safe_cutpoints", args.stop_after):
            status.update(
                {
                    "ok": True,
                    "stage": "safe_cutpoints",
                    "summary": safe_cutpoint_summary,
                    "completed_at": utc_now_iso(),
                }
            )
            write_status(run_root, status)
            log_line(run_root, "dataset worker completed SafeCutPoint diagnostics")
            return 0

        status.update({"stage": "candidate_review_clips", "summary": safe_cutpoint_summary})
        write_status(run_root, status)
        candidate_review_summary = assemble_candidate_review_clips(run_root, config)
        log_line(run_root, f"candidate_review_clips completed summary={candidate_review_summary}")
        if should_stop("candidate_review_clips", args.stop_after):
            status.update(
                {
                    "ok": True,
                    "stage": "candidate_review_clips",
                    "summary": candidate_review_summary,
                    "completed_at": utc_now_iso(),
                }
            )
            write_status(run_root, status)
            log_line(run_root, "dataset worker completed candidate review clips")
            return 0

        status.update({"stage": "native_export", "summary": candidate_review_summary})
        write_status(run_root, status)
        native_export_summary = export_native_candidate_clips(run_root, config)
        log_line(run_root, f"native_export completed summary={native_export_summary}")
        status.update(
            {
                "ok": True,
                "stage": "native_export",
                "summary": native_export_summary,
                "completed_at": utc_now_iso(),
            }
        )
        write_status(run_root, status)
        log_line(run_root, "dataset worker completed native export")
        return 0
    except Exception as exc:
        status.update(
            {
                "ok": False,
                "stage": status.get("stage") or "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "reason_codes": failure_reason_codes(str(status.get("stage") or "failed"), exc),
                "completed_at": utc_now_iso(),
            }
        )
        write_status(run_root, status)
        log_line(run_root, f"dataset worker failed: {status['error']}")
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SpeechCraft dataset worker")
    parser.add_argument("--run-root", required=True, help="Run root where status, logs, and artifacts are written")
    parser.add_argument("--source-wav", action="append", default=[], help="Source WAV. Repeat for multi-WAV runs.")
    parser.add_argument("--config", default=None, help="Optional JSON config override")
    parser.add_argument("--single-speaker", action="store_true", help="Skip diarization stages in later pipeline steps")
    parser.add_argument("--target-speaker-label", default="speaker_0")
    parser.add_argument(
        "--stop-after",
        choices=[
            "source_audio",
            "audio_variants",
            "vad",
            "diarization",
            "buffers",
            "asr_queue",
            "asr",
            "normalization",
            "mfa",
            "alignment_qc",
            "safe_cutpoints",
            "candidate_review_clips",
            "native_export",
        ],
        default="alignment_qc",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return run_dataset_worker(args)


if __name__ == "__main__":
    raise SystemExit(main())
