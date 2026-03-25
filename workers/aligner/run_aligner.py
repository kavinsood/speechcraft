from __future__ import annotations

import argparse
import json
import re
import wave
from pathlib import Path

import numpy as np
import torch
import torchaudio
import torchaudio.functional as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run torchaudio CTC forced alignment.")
    parser.add_argument("--audio", required=True, help="Path to the input WAV file.")
    parser.add_argument(
        "--text",
        required=True,
        help="Transcript string or a path to a text file containing the transcript.",
    )
    parser.add_argument("--output", required=True, help="Path to write the alignment JSON.")
    return parser.parse_args()


def resolve_text_argument(raw_text: str) -> str:
    candidate = Path(raw_text)
    try:
        if candidate.exists() and candidate.is_file():
            return candidate.read_text(encoding="utf-8").strip()
    except OSError:
        pass
    return raw_text.strip()


def select_bundle() -> object:
    return getattr(torchaudio.pipelines, "MMS_FA_EN", torchaudio.pipelines.MMS_FA)


def get_model(bundle: object, device: torch.device) -> torch.nn.Module:
    try:
        model = bundle.get_model(with_star=False)
    except TypeError:
        model = bundle.get_model()
    return model.to(device)


ONES = {
    0: "zero",
    1: "one",
    2: "two",
    3: "three",
    4: "four",
    5: "five",
    6: "six",
    7: "seven",
    8: "eight",
    9: "nine",
    10: "ten",
    11: "eleven",
    12: "twelve",
    13: "thirteen",
    14: "fourteen",
    15: "fifteen",
    16: "sixteen",
    17: "seventeen",
    18: "eighteen",
    19: "nineteen",
}

TENS = {
    20: "twenty",
    30: "thirty",
    40: "forty",
    50: "fifty",
    60: "sixty",
    70: "seventy",
    80: "eighty",
    90: "ninety",
}


def normalize_words(text: str, dictionary: dict[str, int]) -> tuple[list[str], list[str], list[int]]:
    original_words = text.split()
    normalized_words: list[str] = []
    retained_indices: list[int] = []
    blank_symbols = {token for token, token_id in dictionary.items() if token_id == 0}
    allowed_chars = set(dictionary.keys()) - {"*", *blank_symbols}

    for index, word in enumerate(original_words):
        cleaned = normalize_word(word, allowed_chars)
        if not cleaned:
            continue
        normalized_words.append(cleaned)
        retained_indices.append(index)

    return original_words, normalized_words, retained_indices


def normalize_word(word: str, allowed_chars: set[str]) -> str:
    cleaned = word.lower().replace("’", "'")
    cleaned = cleaned.replace("%", "percent")
    cleaned = cleaned.replace("&", "and")
    cleaned = cleaned.replace("+", "plus")
    cleaned = cleaned.replace("@", "at")
    cleaned = cleaned.replace("—", "")
    cleaned = cleaned.replace("-", "")
    cleaned = re.sub(r"\d+", lambda match: integer_to_words(int(match.group(0))), cleaned)
    cleaned = "".join(character for character in cleaned if character in allowed_chars)
    return cleaned


def integer_to_words(value: int) -> str:
    if value < 20:
        return ONES[value]
    if value < 100:
        tens = (value // 10) * 10
        remainder = value % 10
        return TENS[tens] + (ONES[remainder] if remainder else "")
    if value < 1000:
        hundreds = value // 100
        remainder = value % 100
        prefix = ONES[hundreds] + "hundred"
        return prefix + (integer_to_words(remainder) if remainder else "")
    if value < 10000:
        thousands = value // 1000
        remainder = value % 1000
        prefix = ONES[thousands] + "thousand"
        return prefix + (integer_to_words(remainder) if remainder else "")
    return "".join(ONES[int(digit)] for digit in str(value))


def unflatten_spans(token_spans: list[object], word_lengths: list[int]) -> list[list[object]]:
    grouped: list[list[object]] = []
    cursor = 0
    for word_length in word_lengths:
        grouped.append(token_spans[cursor : cursor + word_length])
        cursor += word_length
    return grouped


def load_pcm_wav(audio_path: Path) -> tuple[torch.Tensor, int]:
    with wave.open(str(audio_path), "rb") as wav_file:
        channels = wav_file.getnchannels()
        sample_width = wav_file.getsampwidth()
        sample_rate = wav_file.getframerate()
        frame_count = wav_file.getnframes()
        raw_frames = wav_file.readframes(frame_count)

    if sample_width != 2:
        raise ValueError("run_aligner.py only supports 16-bit PCM WAV input")

    pcm = np.frombuffer(raw_frames, dtype="<i2").astype(np.float32)
    if frame_count == 0:
        waveform = torch.zeros((1, 0), dtype=torch.float32)
        return waveform, sample_rate

    if channels > 1:
        pcm = pcm.reshape(frame_count, channels).mean(axis=1)

    waveform = torch.from_numpy(pcm / 32768.0).unsqueeze(0)
    return waveform, sample_rate


def align_words(audio_path: Path, transcript_text: str) -> list[dict[str, float | str]]:
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    bundle = select_bundle()
    model = get_model(bundle, device)
    dictionary = bundle.get_dict()

    waveform, sample_rate = load_pcm_wav(audio_path)
    if sample_rate != bundle.sample_rate:
        waveform = F.resample(waveform, sample_rate, bundle.sample_rate)
        sample_rate = bundle.sample_rate

    original_words, normalized_words, retained_indices = normalize_words(transcript_text, dictionary)
    token_ids = [dictionary[character] for word in normalized_words for character in word]

    with torch.inference_mode():
        emission, _ = model(waveform.to(device))

    audio_duration = waveform.size(1) / sample_rate if sample_rate > 0 else 0.0
    if not token_ids:
        return fallback_even_word_spans(original_words, audio_duration)

    targets = torch.tensor([token_ids], dtype=torch.int32, device=device)
    alignments, scores = F.forced_align(emission, targets, blank=0)
    alignments = alignments[0]
    scores = scores[0].exp()
    token_spans = F.merge_tokens(alignments, scores)
    word_spans = unflatten_spans(token_spans, [len(word) for word in normalized_words])

    ratio = waveform.size(1) / emission.size(1) / sample_rate
    aligned_subset: dict[int, dict[str, float | str]] = {}
    for original_index, spans in zip(retained_indices, word_spans):
        start_seconds = spans[0].start * ratio
        end_seconds = spans[-1].end * ratio
        aligned_subset[original_index] = {
            "word": original_words[original_index],
            "start": round(float(start_seconds), 6),
            "end": round(float(end_seconds), 6),
        }

    return restore_skipped_words(original_words, aligned_subset, audio_duration)


def fallback_even_word_spans(original_words: list[str], audio_duration: float) -> list[dict[str, float | str]]:
    if not original_words:
        return []
    step = audio_duration / len(original_words) if original_words else 0.0
    aligned_words: list[dict[str, float | str]] = []
    for index, word in enumerate(original_words):
        start = step * index
        end = step * (index + 1)
        aligned_words.append(
            {
                "word": word,
                "start": round(float(start), 6),
                "end": round(float(end), 6),
                "interpolated": True,
            }
        )
    return aligned_words


def restore_skipped_words(
    original_words: list[str],
    aligned_subset: dict[int, dict[str, float | str]],
    audio_duration: float,
) -> list[dict[str, float | str]]:
    if len(aligned_subset) == len(original_words):
        return [aligned_subset[index] for index in range(len(original_words))]

    restored: list[dict[str, float | str] | None] = [None] * len(original_words)
    for index, payload in aligned_subset.items():
        restored[index] = payload

    cursor = 0
    while cursor < len(original_words):
        if restored[cursor] is not None:
            cursor += 1
            continue

        run_start = cursor
        while cursor < len(original_words) and restored[cursor] is None:
            cursor += 1
        run_end = cursor - 1

        previous_index = run_start - 1
        next_index = cursor
        start_seconds = float(restored[previous_index]["end"]) if previous_index >= 0 and restored[previous_index] else 0.0
        end_seconds = (
            float(restored[next_index]["start"])
            if next_index < len(restored) and restored[next_index] is not None
            else audio_duration
        )
        if end_seconds < start_seconds:
            end_seconds = start_seconds

        count = run_end - run_start + 1
        step = (end_seconds - start_seconds) / count if count > 0 else 0.0
        for offset, word_index in enumerate(range(run_start, run_end + 1)):
            word_start = start_seconds + step * offset
            word_end = start_seconds + step * (offset + 1) if step > 0 else start_seconds
            restored[word_index] = {
                "word": original_words[word_index],
                "start": round(float(word_start), 6),
                "end": round(float(word_end), 6),
                "interpolated": True,
            }

    return [item for item in restored if item is not None]


def main() -> None:
    args = parse_args()
    audio_path = Path(args.audio).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    transcript_text = resolve_text_argument(args.text)
    alignment = align_words(audio_path, transcript_text)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(alignment), encoding="utf-8")


if __name__ == "__main__":
    main()
