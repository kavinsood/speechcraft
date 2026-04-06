from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
import wave
from pathlib import Path
from typing import Any

import numpy as np

from app.slicer_algo import SlicerConfig, plan_slices


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate the standalone slicer algorithm on a local WAV.")
    parser.add_argument("--audio", required=True, help="Path to a PCM WAV file.")
    parser.add_argument("--alignment-json", help="Existing alignment JSON to use.")
    parser.add_argument("--transcript-file", help="Transcript text file used to generate alignment.")
    parser.add_argument("--transcript-text", help="Inline transcript text used to generate alignment.")
    parser.add_argument("--target-dur", type=float, default=7.0)
    parser.add_argument("--min-dur", type=float, default=2.0)
    parser.add_argument("--max-dur", type=float, default=15.0)
    parser.add_argument("--soft-max", type=float, default=10.0)
    parser.add_argument("--padding-ms", type=float, default=150.0)
    parser.add_argument("--snap-collar-ms", type=float, default=150.0)
    parser.add_argument("--write-result", help="Optional path to write the full slicer result JSON.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    audio_path = Path(args.audio).expanduser().resolve()
    if not audio_path.exists():
        raise SystemExit(f"Audio file not found: {audio_path}")

    alignment = load_alignment(
        audio_path=audio_path,
        alignment_path=Path(args.alignment_json).expanduser().resolve() if args.alignment_json else None,
        transcript_file=Path(args.transcript_file).expanduser().resolve() if args.transcript_file else None,
        transcript_text=args.transcript_text,
    )
    audio_samples, sample_rate = load_pcm_wav(audio_path)
    result = plan_slices(
        alignment,
        audio_samples,
        sample_rate,
        SlicerConfig(
            target_duration=args.target_dur,
            min_duration=args.min_dur,
            max_duration=args.max_dur,
            soft_max=args.soft_max,
            padding_ms=args.padding_ms,
            snap_collar_ms=args.snap_collar_ms,
        ),
    )

    if args.write_result:
        output_path = Path(args.write_result).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print(json.dumps(result["stats"], indent=2))


def load_alignment(
    *,
    audio_path: Path,
    alignment_path: Path | None,
    transcript_file: Path | None,
    transcript_text: str | None,
) -> list[dict[str, Any]]:
    if alignment_path is not None:
        payload = json.loads(alignment_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("Alignment JSON must be a list of word timing objects")
        return payload

    transcript = resolve_transcript(transcript_file, transcript_text)
    if not transcript:
        raise ValueError("Provide --alignment-json or a transcript via --transcript-file/--transcript-text")
    return run_aligner_worker(audio_path, transcript)


def resolve_transcript(transcript_file: Path | None, transcript_text: str | None) -> str:
    if transcript_text and transcript_text.strip():
        return transcript_text.strip()
    if transcript_file is not None:
        return transcript_file.read_text(encoding="utf-8").strip()
    return ""


def run_aligner_worker(audio_path: Path, transcript: str) -> list[dict[str, Any]]:
    repo_root = Path(__file__).resolve().parents[2]
    worker_python = repo_root / "workers" / "aligner" / ".venv" / "bin" / "python"
    worker_script = repo_root / "workers" / "aligner" / "run_aligner.py"
    if not worker_python.exists():
        raise ValueError(f"Aligner worker python not found: {worker_python}")
    if not worker_script.exists():
        raise ValueError(f"Aligner worker script not found: {worker_script}")

    with tempfile.TemporaryDirectory(prefix="speechcraft-slicer-eval-") as temp_dir_raw:
        output_path = Path(temp_dir_raw) / "alignment.json"
        completed = subprocess.run(
            [
                str(worker_python),
                str(worker_script),
                "--audio",
                str(audio_path),
                "--text",
                transcript,
                "--output",
                str(output_path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        if not output_path.exists():
            detail = (completed.stderr or completed.stdout or "").strip()
            raise ValueError(detail or "Aligner worker did not produce alignment output")
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("Aligner worker returned invalid alignment JSON")
        return payload


def load_pcm_wav(audio_path: Path) -> tuple[np.ndarray, int]:
    with wave.open(str(audio_path), "rb") as wav_file:
        channels = wav_file.getnchannels()
        sample_width = wav_file.getsampwidth()
        sample_rate = wav_file.getframerate()
        frame_count = wav_file.getnframes()
        raw_frames = wav_file.readframes(frame_count)

    if sample_width != 2:
        raise ValueError("evaluate_slicer_algo.py only supports 16-bit PCM WAV input")

    audio = np.frombuffer(raw_frames, dtype="<i2").astype(np.float64)
    if frame_count == 0:
        return np.zeros(0, dtype=np.float64), sample_rate
    if channels > 1:
        audio = audio.reshape(frame_count, channels).mean(axis=1)
    return audio / 32768.0, sample_rate


if __name__ == "__main__":
    main()
