from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .io import read_json_value, resolve_under_root, sha256_file, write_json


DANGER_SYMBOLS = set("$%@#*+=&€£¥©™^_")


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
        for token in re.sub(r"[^a-z0-9'\s]", " ", raw.lower()).split()
        if token.strip("'")
    ]
    reasons = hazard_reason_codes(symbols, contains_numeric)
    if symbols and not alignment_tokens:
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
        "original_text": text,
        "raw_asr_text": text,
        "training_text": text,
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


def normalize_transcripts(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    transcripts_path = resolve_under_root(run_root, "artifacts/transcripts.json")
    transcripts = list(read_json_value(transcripts_path))
    normalized_rows: list[dict[str, Any]] = []
    for row in transcripts:
        normalized = normalize_for_mfa(str(row.get("text") or ""))
        normalized_rows.append(
            {
                "buffer_id": row["buffer_id"],
                "audio_path": row["audio_path"],
                **normalized,
            }
        )
    normalized_path = resolve_under_root(run_root, "artifacts/normalized_transcripts.json")
    hazards_path = resolve_under_root(run_root, "artifacts/transcript_hazards.json")
    write_json(normalized_path, normalized_rows)
    write_json(hazards_path, normalized_rows)
    symbol_buffer_counts = {
        symbol: sum(symbol in row["symbols"] for row in normalized_rows)
        for symbol in sorted({symbol for row in normalized_rows for symbol in row["symbols"]})
    }
    hazard_summary = {
        "buffer_count": len(normalized_rows),
        "buffers_with_symbol_hazards": sum(bool(row["symbols"]) for row in normalized_rows),
        "buffers_with_numeric_tokens": sum(bool(row["numeric_tokens"]) for row in normalized_rows),
        "buffers_requiring_review": sum(row["needs_review"] for row in normalized_rows),
        "buffers_with_automatic_cutpoints_disabled": sum(row["disable_automatic_cutpoints"] for row in normalized_rows),
        "symbol_buffer_counts": symbol_buffer_counts,
        "numeric_token_count": sum(len(row["numeric_tokens"]) for row in normalized_rows),
        "symbol_only_tokens": sum(
            "symbol_only_token_stripped" in token["reason_codes"]
            for row in normalized_rows
            for token in row["token_hazards"]
        ),
    }
    write_json(resolve_under_root(run_root, "artifacts/symbol_hazard_summary.json"), hazard_summary)
    summary = {
        "stage": "normalization",
        "config_hash": str(config.get("config_hash") or ""),
        "input_artifact_hashes": {
            "transcripts_json": sha256_file(transcripts_path),
        },
        "output_hashes": {
            "normalized_transcripts_json": sha256_file(normalized_path),
            "transcript_hazards_json": sha256_file(hazards_path),
        },
        "empty_normalized_transcripts": sum(not row["normalized_text"] for row in normalized_rows),
        "total_tokens": sum(len(row["tokens"]) for row in normalized_rows),
        **hazard_summary,
    }
    write_json(resolve_under_root(run_root, "artifacts/normalization_summary.json"), summary)
    return summary
