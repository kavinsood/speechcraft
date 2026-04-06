from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
import wave
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run backend-style faster-whisper ASR and aligner-worker forced alignment over every WAV in a folder."
    )
    parser.add_argument("--input-dir", required=True, help="Directory containing source WAV files.")
    parser.add_argument("--output-dir", help="Directory to write ASR/alignment artifacts.")
    parser.add_argument("--glob", default="*.wav", help="Glob used to select audio files inside input-dir.")
    parser.add_argument("--language-hint", help="Optional language hint passed through to ASR.")
    parser.add_argument("--model-name", help="Optional override for output metadata.")
    parser.add_argument("--model-version", help="Optional override for output metadata.")
    parser.add_argument(
        "--allow-stub",
        action="store_true",
        help="Allow the backend stub ASR mode instead of requiring ASR_BACKEND=faster_whisper.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir).expanduser().resolve()
    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input directory not found: {input_dir}")

    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else Path(__file__).resolve().parents[1] / "exports" / "folder-asr-align" / input_dir.name
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    audio_paths = sorted(path for path in input_dir.glob(args.glob) if path.is_file())
    if not audio_paths:
        raise SystemExit(f"No files matched {args.glob!r} under {input_dir}")

    adapter_config = review_window_asr_backend_config(
        {
            "model_name": args.model_name,
            "model_version": args.model_version,
        }
    )
    if adapter_config["backend"] == "stub" and not args.allow_stub:
        raise SystemExit(
            "ASR resolved to the backend stub adapter. Set ASR_BACKEND=faster_whisper and "
            "ASR_MODEL_PATH=/abs/path/to/local/model, or rerun with --allow-stub if that is intentional."
        )

    backend_client = create_asr_backend_client(adapter_config)
    summary: list[dict[str, Any]] = []

    for audio_path in audio_paths:
        result = process_audio_file(
            audio_path=audio_path,
            output_dir=output_dir,
            adapter_config=adapter_config,
            backend_client=backend_client,
            language_hint=args.language_hint,
        )
        summary.append(result)
        print(
            json.dumps(
                {
                    "file": audio_path.name,
                    "status": result["status"],
                    "transcript_words": result.get("transcript_word_count", 0),
                    "alignment_words": result.get("alignment_word_count", 0),
                }
            )
        )

    (output_dir / "_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")


def process_audio_file(
    *,
    audio_path: Path,
    output_dir: Path,
    adapter_config: dict[str, str],
    backend_client: Any,
    language_hint: str | None,
) -> dict[str, Any]:
    stem = audio_path.stem
    asr_output_path = output_dir / f"{stem}.asr.json"
    alignment_output_path = output_dir / f"{stem}.alignment.json"
    transcript_output_path = output_dir / f"{stem}.transcript.txt"
    error_output_path = output_dir / f"{stem}.error.json"
    asr_result: dict[str, Any] | None = None

    try:
        asr_result = run_asr_on_audio_file(
            audio_path=audio_path,
            adapter_config=adapter_config,
            backend_client=backend_client,
            language_hint=language_hint,
        )
        transcript_text = str(asr_result.get("transcript_text") or "").strip()
        if not transcript_text:
            raise ValueError("ASR returned an empty transcript")

        asr_output_path.write_text(json.dumps(asr_result, indent=2), encoding="utf-8")
        transcript_output_path.write_text(transcript_text + "\n", encoding="utf-8")

        alignment = run_forced_align_worker(audio_path.read_bytes(), transcript_text)

        alignment_output_path.write_text(json.dumps(alignment, indent=2), encoding="utf-8")

        return {
            "file": audio_path.name,
            "status": "ok",
            "asr_path": str(asr_output_path),
            "alignment_path": str(alignment_output_path),
            "transcript_path": str(transcript_output_path),
            "transcript_word_count": len(transcript_text.split()),
            "alignment_word_count": len(alignment),
        }
    except Exception as exc:
        error_payload = {
            "file": audio_path.name,
            "status": "error",
            "error": str(exc),
        }
        if asr_result is not None and not asr_output_path.exists():
            asr_output_path.write_text(json.dumps(asr_result, indent=2), encoding="utf-8")
        error_output_path.write_text(json.dumps(error_payload, indent=2), encoding="utf-8")
        return error_payload


def review_window_asr_backend_config(payload: dict[str, Any]) -> dict[str, str]:
    backend = str(os.getenv("ASR_BACKEND", "stub")).strip().lower() or "stub"
    requested_model_name = str(payload.get("model_name") or "").strip()
    requested_model_version = str(payload.get("model_version") or "").strip()
    if backend == "stub":
        return {
            "backend": "stub",
            "model_name": requested_model_name or "stub-review-window-asr",
            "model_version": requested_model_version or "stub-v1",
        }
    if backend == "faster_whisper":
        model_path = str(os.getenv("ASR_MODEL_PATH", "")).strip()
        if not model_path:
            raise ValueError("ASR_MODEL_PATH is required when ASR_BACKEND=faster_whisper")
        model_path_obj = Path(model_path)
        if not model_path_obj.exists():
            raise ValueError(f"ASR_MODEL_PATH does not exist: {model_path_obj}")
        return {
            "backend": "faster_whisper",
            "model_path": str(model_path_obj),
            "model_name": requested_model_name or model_path_obj.name,
            "model_version": requested_model_version or "local",
            "device": str(os.getenv("ASR_DEVICE", "cpu")).strip() or "cpu",
            "compute_type": str(os.getenv("ASR_COMPUTE_TYPE", "int8")).strip() or "int8",
        }
    raise ValueError(f"Unsupported ASR_BACKEND: {backend}")


def create_asr_backend_client(adapter_config: dict[str, str]) -> Any:
    backend = adapter_config["backend"]
    if backend == "stub":
        return None
    if backend == "faster_whisper":
        return load_faster_whisper_model(
            model_path=adapter_config["model_path"],
            device=adapter_config["device"],
            compute_type=adapter_config["compute_type"],
        )
    raise ValueError(f"Unsupported ASR backend: {backend}")


def load_faster_whisper_model(*, model_path: str, device: str, compute_type: str) -> Any:
    try:
        from faster_whisper import WhisperModel
    except ImportError as exc:
        raise ValueError(
            "ASR_BACKEND=faster_whisper requires the faster-whisper package in the local environment"
        ) from exc
    return WhisperModel(model_path, device=device, compute_type=compute_type)


def run_asr_on_audio_file(
    *,
    audio_path: Path,
    adapter_config: dict[str, str],
    backend_client: Any,
    language_hint: str | None,
) -> dict[str, Any]:
    backend = adapter_config["backend"]
    if backend == "stub":
        duration_seconds = wav_duration_seconds(audio_path)
        transcript_text = f"stub asr {audio_path.stem}"
        return {
            "backend": "stub",
            "transcript_text": transcript_text,
            "model_name": adapter_config["model_name"],
            "model_version": adapter_config["model_version"],
            "language": language_hint or "",
            "segments": [
                {
                    "start": 0.0,
                    "end": round(duration_seconds, 6),
                    "text": transcript_text,
                }
            ],
        }
    if backend == "faster_whisper":
        model = backend_client
        segments_iter, info = model.transcribe(
            str(audio_path),
            language=language_hint or None,
            condition_on_previous_text=False,
        )
        segments = [
            {
                "start": round(float(segment.start), 6),
                "end": round(float(segment.end), 6),
                "text": str(segment.text).strip(),
            }
            for segment in segments_iter
        ]
        transcript_text = " ".join(
            segment["text"] for segment in segments if str(segment.get("text") or "").strip()
        ).strip()
        return {
            "backend": "faster_whisper",
            "transcript_text": transcript_text,
            "model_name": adapter_config["model_name"],
            "model_version": adapter_config["model_version"],
            "language": str(getattr(info, "language", "") or language_hint or ""),
            "segments": segments,
        }
    raise ValueError(f"Unsupported ASR backend: {backend}")


def run_forced_align_worker(audio_bytes: bytes, transcript_text: str) -> list[dict[str, Any]]:
    if not transcript_text.strip():
        raise ValueError("Forced align worker requires transcript_text")

    worker_python, worker_script = resolve_forced_align_worker_paths()
    with tempfile.TemporaryDirectory(prefix="speechcraft-aligner-") as temp_dir_raw:
        temp_dir = Path(temp_dir_raw)
        audio_path = temp_dir / "input.wav"
        output_path = temp_dir / "alignment.json"
        transcript_path = temp_dir / "transcript.txt"
        audio_path.write_bytes(audio_bytes)
        transcript_path.write_text(transcript_text, encoding="utf-8")

        try:
            completed = subprocess.run(
                [
                    str(worker_python),
                    str(worker_script),
                    "--audio",
                    str(audio_path),
                    "--text",
                    str(transcript_path),
                    "--output",
                    str(output_path),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or exc.stdout or "").strip()
            raise ValueError(stderr or "Forced align worker failed") from exc
        except OSError as exc:
            raise ValueError(f"Forced align worker failed to launch: {exc}") from exc

        if not output_path.exists():
            stdout = (completed.stdout or "").strip()
            stderr = (completed.stderr or "").strip()
            detail = stderr or stdout or "Forced align worker did not produce alignment output"
            raise ValueError(detail)

        alignment_payload = json.loads(output_path.read_text(encoding="utf-8"))

    if not isinstance(alignment_payload, list):
        raise ValueError("Forced align worker returned invalid alignment payload")
    return alignment_payload


def resolve_forced_align_worker_paths() -> tuple[Path, Path]:
    repo_root = Path(__file__).resolve().parents[2]
    worker_root = repo_root / "workers" / "aligner"
    worker_python = worker_root / ".venv" / "bin" / "python"
    worker_script = worker_root / "run_aligner.py"

    if not worker_python.exists():
        raise ValueError(f"Forced align worker python not found: {worker_python}")
    if not worker_script.exists():
        raise ValueError(f"Forced align worker script not found: {worker_script}")
    return worker_python, worker_script


def wav_duration_seconds(audio_path: Path) -> float:
    with wave.open(str(audio_path), "rb") as wav_file:
        frame_rate = wav_file.getframerate()
        frame_count = wav_file.getnframes()
    return frame_count / frame_rate if frame_rate > 0 else 0.0


if __name__ == "__main__":
    main()
