from __future__ import annotations

import argparse
import json
import tempfile
import wave
from pathlib import Path
from typing import Any

from run_folder_asr_align import run_forced_align_worker


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run forced alignment over an existing transcript JSON with timed segments and merge word timings."
    )
    parser.add_argument("--audio", required=True, help="Path to a 16-bit PCM WAV file.")
    parser.add_argument("--transcript-json", required=True, help="Path to an ASR-style transcript JSON containing segments.")
    parser.add_argument("--output", required=True, help="Path to write merged alignment JSON.")
    parser.add_argument("--summary-output", help="Optional path to write a summary JSON.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    audio_path = Path(args.audio).expanduser().resolve()
    transcript_json_path = Path(args.transcript_json).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    summary_output_path = (
        Path(args.summary_output).expanduser().resolve() if args.summary_output else output_path.with_name("_summary.json")
    )

    if not audio_path.exists():
        raise SystemExit(f"Audio file not found: {audio_path}")
    if not transcript_json_path.exists():
        raise SystemExit(f"Transcript JSON not found: {transcript_json_path}")

    transcript_payload = json.loads(transcript_json_path.read_text(encoding="utf-8"))
    segments = transcript_payload.get("segments")
    if not isinstance(segments, list):
        raise SystemExit(f"Transcript JSON missing segments array: {transcript_json_path}")

    sample_rate, channels, sample_width, pcm_bytes, frame_count = read_wav(audio_path)
    audio_duration = frame_count / sample_rate if sample_rate > 0 else 0.0

    merged_alignment: list[dict[str, Any]] = []
    processed_segments = 0
    skipped_segments = 0

    for index, segment in enumerate(segments):
        if not isinstance(segment, dict):
            skipped_segments += 1
            continue
        text = str(segment.get("text") or "").strip()
        start = max(0.0, float(segment.get("start") or 0.0))
        end = min(audio_duration, float(segment.get("end") or 0.0))
        if not text or end <= start:
            skipped_segments += 1
            continue

        chunk_bytes = slice_wav_bytes(
            pcm_bytes=pcm_bytes,
            sample_rate=sample_rate,
            channels=channels,
            sample_width=sample_width,
            start_seconds=start,
            end_seconds=end,
        )
        if not chunk_bytes:
            skipped_segments += 1
            continue

        with tempfile.TemporaryDirectory(prefix="speechcraft-segment-align-") as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            chunk_path = temp_dir / f"segment-{index:04d}.wav"
            chunk_path.write_bytes(chunk_bytes)
            alignment = run_forced_align_worker(chunk_bytes, text)

        for word in alignment:
            merged_alignment.append(
                {
                    "word": word["word"],
                    "start": round(start + float(word["start"]), 6),
                    "end": round(start + float(word["end"]), 6),
                    **({"interpolated": True} if bool(word.get("interpolated")) else {}),
                }
            )
        processed_segments += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(merged_alignment, indent=2), encoding="utf-8")

    summary = {
        "file": audio_path.name,
        "status": "ok",
        "audio_path": str(audio_path),
        "transcript_json_path": str(transcript_json_path),
        "alignment_path": str(output_path),
        "audio_duration_s": round(audio_duration, 6),
        "segment_count": len(segments),
        "processed_segments": processed_segments,
        "skipped_segments": skipped_segments,
        "alignment_word_count": len(merged_alignment),
    }
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


def read_wav(audio_path: Path) -> tuple[int, int, int, bytes, int]:
    with wave.open(str(audio_path), "rb") as wav_file:
        channels = wav_file.getnchannels()
        sample_width = wav_file.getsampwidth()
        sample_rate = wav_file.getframerate()
        frame_count = wav_file.getnframes()
        pcm_bytes = wav_file.readframes(frame_count)
    if sample_width != 2:
        raise SystemExit("align_existing_segments.py only supports 16-bit PCM WAV input")
    return sample_rate, channels, sample_width, pcm_bytes, frame_count


def slice_wav_bytes(
    *,
    pcm_bytes: bytes,
    sample_rate: int,
    channels: int,
    sample_width: int,
    start_seconds: float,
    end_seconds: float,
) -> bytes:
    bytes_per_frame = channels * sample_width
    start_frame = max(0, int(round(start_seconds * sample_rate)))
    end_frame = max(start_frame, int(round(end_seconds * sample_rate)))
    chunk = pcm_bytes[start_frame * bytes_per_frame : end_frame * bytes_per_frame]
    if not chunk:
        return b""

    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temp_file:
        temp_path = Path(temp_file.name)
    try:
        with wave.open(str(temp_path), "wb") as wav_file:
            wav_file.setnchannels(channels)
            wav_file.setsampwidth(sample_width)
            wav_file.setframerate(sample_rate)
            wav_file.writeframes(chunk)
        return temp_path.read_bytes()
    finally:
        temp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
