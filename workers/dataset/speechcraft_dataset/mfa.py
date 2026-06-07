from __future__ import annotations

import os
import re
import shutil
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any

from .io import read_json_value, read_jsonl, resolve_under_root, sha256_file, write_json, write_jsonl


DEFAULT_MFA_DICTIONARY = "english_us_mfa"
DEFAULT_MFA_ACOUSTIC_MODEL = "english_mfa"
ANALYSIS_SAMPLE_RATE = 16000


def build_mfa_corpus(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    normalized_path = resolve_under_root(run_root, "artifacts/normalized_transcripts.json")
    queue_path = resolve_under_root(run_root, "artifacts/asr_mfa_queue.json")
    normalized_rows = list(read_json_value(normalized_path))
    queue_by_id = {row["buffer_id"]: row for row in read_json_value(queue_path)}
    corpus_dir = resolve_under_root(run_root, "artifacts/mfa_corpus")
    if corpus_dir.exists():
        shutil.rmtree(corpus_dir)
    corpus_dir.mkdir(parents=True, exist_ok=True)

    included: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in normalized_rows:
        buffer_id = str(row["buffer_id"])
        alignment_text = str(row.get("alignment_text") or "").strip()
        queue_row = queue_by_id.get(buffer_id)
        if queue_row is None:
            skipped.append({"buffer_id": buffer_id, "reason_codes": ["missing_asr_mfa_queue_row"]})
            continue
        if not alignment_text:
            skipped.append({"buffer_id": buffer_id, "reason_codes": ["empty_alignment_text"]})
            continue
        source_audio_path = resolve_under_root(run_root, str(queue_row.get("queue_audio_path") or queue_row["audio_path"]))
        wav_path = corpus_dir / f"{buffer_id}.wav"
        lab_path = corpus_dir / f"{buffer_id}.lab"
        shutil.copy2(source_audio_path, wav_path)
        lab_path.write_text(alignment_text + "\n", encoding="utf-8")
        included.append(
            {
                "buffer_id": buffer_id,
                "wav_path": str(wav_path.relative_to(run_root)),
                "lab_path": str(lab_path.relative_to(run_root)),
                "alignment_token_count": len(row.get("alignment_tokens") or []),
                "audio_hash": sha256_file(wav_path),
                "lab_hash": sha256_file(lab_path),
            }
        )

    manifest_path = resolve_under_root(run_root, "artifacts/mfa_corpus_manifest.json")
    write_json(
        manifest_path,
        {
            "stage": "mfa_corpus",
            "config_hash": str(config.get("config_hash") or ""),
            "input_artifact_hashes": {
                "normalized_transcripts_json": sha256_file(normalized_path),
                "asr_mfa_queue_json": sha256_file(queue_path),
            },
            "corpus_dir": "artifacts/mfa_corpus",
            "included": included,
            "skipped": skipped,
        },
    )
    return {
        "stage": "mfa_corpus",
        "config_hash": str(config.get("config_hash") or ""),
        "input_artifact_hashes": {
            "normalized_transcripts_json": sha256_file(normalized_path),
            "asr_mfa_queue_json": sha256_file(queue_path),
        },
        "output_hashes": {"mfa_corpus_manifest_json": sha256_file(manifest_path)},
        "included_utterances": len(included),
        "skipped_utterances": len(skipped),
        "corpus_dir": "artifacts/mfa_corpus",
    }


def mfa_binary(config: dict[str, Any]) -> str:
    configured = str(config.get("mfa_bin") or os.environ.get("SPEECHCRAFT_MFA_BIN") or "").strip()
    if configured:
        if "/" in configured or "\\" in configured:
            path = Path(configured).expanduser()
            if not path.exists():
                raise RuntimeError(f"MFA binary path does not exist: {path}")
            if not path.is_file():
                raise RuntimeError(f"MFA binary path is not a file: {path}")
            return str(path.resolve())
        binary = shutil.which(configured)
        if not binary:
            raise RuntimeError(f"MFA binary not found on PATH: {configured}")
        return binary
    binary = shutil.which("mfa")
    if not binary:
        raise RuntimeError("MFA binary not configured and not found on PATH")
    return binary


def mfa_runtime_env(config: dict[str, Any], *, binary: str | None = None) -> dict[str, str]:
    """Build subprocess env for MFA, including OpenFST tools next to the MFA binary."""
    env = dict(os.environ)
    binary_path = binary or mfa_binary(config)
    bin_dir = str(Path(binary_path).resolve().parent)
    path_entries = [entry for entry in env.get("PATH", "").split(os.pathsep) if entry]
    if bin_dir not in path_entries:
        env["PATH"] = os.pathsep.join([bin_dir, *path_entries])
    mfa_root_dir = str(config.get("mfa_root_dir") or os.environ.get("SPEECHCRAFT_MFA_ROOT_DIR") or "").strip()
    if mfa_root_dir:
        root = Path(mfa_root_dir).expanduser()
        root.mkdir(parents=True, exist_ok=True)
        env["MFA_ROOT_DIR"] = str(root.resolve())
    env.setdefault("TMPDIR", "/tmp")
    return env


def run_mfa_command(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    binary = mfa_binary(config)
    corpus_dir = resolve_under_root(run_root, "artifacts/mfa_corpus")
    output_dir = resolve_under_root(run_root, "artifacts/mfa_output")
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    dictionary = str(config.get("mfa_dictionary") or os.environ.get("SPEECHCRAFT_MFA_DICTIONARY") or DEFAULT_MFA_DICTIONARY)
    acoustic_model = str(
        config.get("mfa_acoustic_model") or os.environ.get("SPEECHCRAFT_MFA_ACOUSTIC_MODEL") or DEFAULT_MFA_ACOUSTIC_MODEL
    )
    timeout_seconds = int(config.get("mfa_timeout_sec") or 3600)
    command = [
        binary,
        "align",
        "--clean",
        "--overwrite",
    ]
    if bool(config.get("mfa_single_speaker", True)):
        command.append("--single_speaker")
    command.extend([str(corpus_dir), dictionary, acoustic_model, str(output_dir)])
    env = mfa_runtime_env(config, binary=binary)
    try:
        completed = subprocess.run(command, text=True, capture_output=True, timeout=timeout_seconds, env=env)
        status = "ok" if completed.returncode == 0 else "failed"
        summary = {
            "stage": "mfa",
            "config_hash": str(config.get("config_hash") or ""),
            "input_artifact_hashes": {
                "mfa_corpus_manifest_json": sha256_file(resolve_under_root(run_root, "artifacts/mfa_corpus_manifest.json")),
            },
            "status": status,
            "returncode": completed.returncode,
            "command": command,
            "stdout_tail": completed.stdout[-4000:],
            "stderr_tail": completed.stderr[-4000:],
            "corpus_dir": "artifacts/mfa_corpus",
            "output_dir": "artifacts/mfa_output",
            "dictionary": dictionary,
            "acoustic_model": acoustic_model,
            "single_speaker": bool(config.get("mfa_single_speaker", True)),
            "mfa_root_dir": env.get("MFA_ROOT_DIR"),
            "reason_codes": [] if completed.returncode == 0 else ["mfa_command_failed"],
        }
    except subprocess.TimeoutExpired as exc:
        summary = {
            "stage": "mfa",
            "config_hash": str(config.get("config_hash") or ""),
            "input_artifact_hashes": {
                "mfa_corpus_manifest_json": sha256_file(resolve_under_root(run_root, "artifacts/mfa_corpus_manifest.json")),
            },
            "status": "failed",
            "returncode": None,
            "command": command,
            "stdout_tail": decode_tail(exc.stdout),
            "stderr_tail": decode_tail(exc.stderr),
            "corpus_dir": "artifacts/mfa_corpus",
            "output_dir": "artifacts/mfa_output",
            "dictionary": dictionary,
            "acoustic_model": acoustic_model,
            "single_speaker": bool(config.get("mfa_single_speaker", True)),
            "mfa_root_dir": env.get("MFA_ROOT_DIR"),
            "reason_codes": ["mfa_timeout"],
        }
    summary_path = resolve_under_root(run_root, "artifacts/mfa_summary.json")
    write_json(summary_path, summary)
    if summary["status"] != "ok":
        raise RuntimeError(f"MFA alignment failed: {summary['reason_codes']}")
    return summary


def decode_tail(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")[-4000:]
    return value[-4000:]


def parse_textgrid_entries(path: Path) -> list[dict[str, Any]]:
    from praatio import textgrid  # type: ignore

    tg = textgrid.openTextgrid(str(path), includeEmptyIntervals=False)
    tier_name = "words" if "words" in tg.tierNames else tg.tierNames[0]
    tier = tg.getTier(tier_name)
    entries: list[dict[str, Any]] = []
    for entry in tier.entries:
        label = str(getattr(entry, "label", "")).strip()
        if not label:
            continue
        start = float(getattr(entry, "start"))
        end = float(getattr(entry, "end"))
        if end <= start:
            continue
        entries.append({"start": start, "end": end, "label": label})
    return entries


def parse_oov_count_file(path: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        parts = line.strip().split()
        if not parts:
            continue
        if len(parts) == 1:
            counts[parts[0].lower()] = max(counts.get(parts[0].lower(), 0), 1)
        elif parts[-1].isdigit():
            counts[" ".join(parts[:-1]).lower()] = max(counts.get(" ".join(parts[:-1]).lower(), 0), int(parts[-1]))
        elif parts[0].isdigit():
            counts[" ".join(parts[1:]).lower()] = max(counts.get(" ".join(parts[1:]).lower(), 0), int(parts[0]))
    return counts


def discover_mfa_oov_artifacts(run_root: Path, config: dict[str, Any]) -> dict[str, list[Path]]:
    run_artifacts = resolve_under_root(run_root, "artifacts")
    search_roots = [run_artifacts]
    mfa_root_dir = str(config.get("mfa_root_dir") or os.environ.get("SPEECHCRAFT_MFA_ROOT_DIR") or "").strip()
    if mfa_root_dir:
        search_roots.append(Path(mfa_root_dir).expanduser())
    names = {
        "oov_counts": ["oov_counts*.txt"],
        "oovs_found": ["oovs_found*.txt"],
        "utterance_oovs": ["utterance_oovs*.txt"],
        "normalize_oov_log": ["normalize_oov.log"],
    }
    discovered: dict[str, list[Path]] = {kind: [] for kind in names}
    snapshot_dir = resolve_under_root(run_root, "artifacts/mfa_oov_artifacts")
    for root in search_roots:
        if not root.exists():
            continue
        for kind, patterns in names.items():
            for pattern in patterns:
                for path in root.rglob(pattern):
                    if snapshot_dir in path.parents:
                        continue
                    if path not in discovered[kind]:
                        discovered[kind].append(path)
    for paths in discovered.values():
        paths.sort(key=lambda path: (run_artifacts not in path.parents, -path.stat().st_mtime, str(path)))
    return discovered


def snapshot_oov_artifacts(run_root: Path, discovered: dict[str, list[Path]]) -> list[str]:
    artifact_dir = resolve_under_root(run_root, "artifacts/mfa_oov_artifacts")
    artifact_dir.mkdir(parents=True, exist_ok=True)
    copied: list[str] = []
    for kind, paths in discovered.items():
        for index, source in enumerate(paths[:3]):
            destination = artifact_dir / f"{kind}_{index:02d}_{source.name}"
            if source.resolve() == destination.resolve():
                continue
            shutil.copy2(source, destination)
            copied.append(str(destination.relative_to(run_root)))
    return copied


def persist_mfa_oov_words(run_root: Path, config: dict[str, Any]) -> set[str]:
    oov_words_path = resolve_under_root(run_root, "artifacts/oov_words.json")
    discovered = discover_mfa_oov_artifacts(run_root, config)
    oov_counts: dict[str, int] = {}
    parser = None
    source_files: list[str] = []
    selected: dict[str, list[Path]] = {kind: [] for kind in discovered}
    used_fallback_root = False
    run_artifacts = resolve_under_root(run_root, "artifacts")
    for kind in ("oov_counts", "oovs_found"):
        for path in discovered[kind]:
            parsed = parse_oov_count_file(path)
            if parsed:
                oov_counts.update(parsed)
                parser = kind
                source_files.append(str(path))
                selected[kind].append(path)
                used_fallback_root = run_artifacts not in path.parents
                break
        if oov_counts:
            break
    if not oov_counts:
        for log_path in discovered["normalize_oov_log"]:
            log_text = log_path.read_text(encoding="utf-8", errors="replace")
            for word, count in re.findall(r"'word': '([^']+)'.*?'count': (\d+)", log_text):
                oov_counts[word.lower()] = max(oov_counts.get(word.lower(), 0), int(count))
            if oov_counts:
                parser = "normalize_oov_log_regex_fallback"
                source_files.append(str(log_path))
                selected["normalize_oov_log"].append(log_path)
                used_fallback_root = run_artifacts not in log_path.parents
                break
    if discovered["utterance_oovs"]:
        selected["utterance_oovs"].append(discovered["utterance_oovs"][0])
    copied = snapshot_oov_artifacts(run_root, selected)
    rows = [
        {"word": word, "count": count, "is_numeric": bool(re.fullmatch(r"\d+(?:[.,]\d+)?", word))}
        for word, count in sorted(oov_counts.items())
    ]
    write_json(oov_words_path, rows)
    summary = {
        "stage": "mfa_oov",
        "oov_source": parser,
        "oov_types": len(oov_counts),
        "oov_tokens": sum(oov_counts.values()),
        "source_files_found": source_files,
        "snapshot_files": copied,
        "used_fallback_root": used_fallback_root,
        "reason_codes": sorted(
            set(
                ([] if source_files else ["mfa_oov_artifacts_not_found"])
                + (["mfa_oov_artifacts_from_fallback_root"] if used_fallback_root else [])
            )
        ),
    }
    write_json(resolve_under_root(run_root, "artifacts/oov_summary.json"), summary)
    return set(oov_counts)


def parse_mfa_textgrids(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    queue_path = resolve_under_root(run_root, "artifacts/asr_mfa_queue.json")
    normalized_path = resolve_under_root(run_root, "artifacts/normalized_transcripts.json")
    queue_by_id = {row["buffer_id"]: row for row in read_json_value(queue_path)}
    normalized_by_id = {row["buffer_id"]: row for row in read_json_value(normalized_path)}
    oov_words = persist_mfa_oov_words(run_root, config)
    output_dir = resolve_under_root(run_root, "artifacts/mfa_output")
    sample_rate = int(config.get("analysis_sample_rate") or ANALYSIS_SAMPLE_RATE)
    expected_buffer_ids = sorted(queue_by_id)
    rows: list[dict[str, Any]] = []
    textgrid_paths = sorted(output_dir.rglob("*.TextGrid"))
    textgrid_stems = sorted(path.stem for path in textgrid_paths)
    missing_textgrid_buffer_ids = sorted(set(expected_buffer_ids) - set(textgrid_stems))
    unexpected_textgrid_stems = sorted(set(textgrid_stems) - set(expected_buffer_ids))
    for textgrid_path in textgrid_paths:
        buffer_id = textgrid_path.stem
        buffer = queue_by_id.get(buffer_id)
        if buffer is None:
            continue
        entries = parse_textgrid_entries(textgrid_path)
        alignment_tokens = list(normalized_by_id.get(buffer_id, {}).get("alignment_tokens") or [])
        mapping_mismatch = len(entries) != len(alignment_tokens)
        if not mapping_mismatch:
            mapping_mismatch = any(
                entry["label"].strip().lower() != token["alignment"] for entry, token in zip(entries, alignment_tokens)
            )
        for index, entry in enumerate(entries):
            label = str(entry["label"]).strip()
            local_start_sample = int(round(float(entry["start"]) * sample_rate))
            local_end_sample = int(round(float(entry["end"]) * sample_rate))
            source_start_sample = int(buffer["source_start_sample"]) + local_start_sample
            source_end_sample = int(buffer["source_start_sample"]) + local_end_sample
            hazard = alignment_tokens[index] if not mapping_mismatch and index < len(alignment_tokens) else {}
            is_oov = label.lower() in oov_words
            review_reasons = list(hazard.get("reason_codes", []))
            if is_oov:
                review_reasons.extend(["contains_oov", "requires_transcript_review"])
            is_numeric_oov = is_oov and bool(re.fullmatch(r"\d+(?:[.,]\d+)?", label))
            if is_numeric_oov:
                review_reasons.append("contains_numeric_oov")
            if mapping_mismatch:
                review_reasons.extend(
                    ["alignment_token_word_mismatch", "buffer_requires_review", "disable_automatic_cutpoints"]
                )
            rows.append(
                {
                    "id": f"{buffer_id}-word-{index:04d}",
                    "buffer_id": buffer_id,
                    "word_index": index,
                    "word": label,
                    "local_start_sample": local_start_sample,
                    "local_end_sample": local_end_sample,
                    "local_start_sec": local_start_sample / sample_rate,
                    "local_end_sec": local_end_sample / sample_rate,
                    "source_start_sample": source_start_sample,
                    "source_end_sample": source_end_sample,
                    "source_start_sec": source_start_sample / sample_rate,
                    "source_end_sec": source_end_sample / sample_rate,
                    "is_oov": is_oov,
                    "is_numeric_oov": is_numeric_oov,
                    "raw_token_id": hazard.get("raw_token_id"),
                    "raw_token": hazard.get("raw"),
                    "contains_numeric": bool(hazard.get("contains_numeric")),
                    "contains_danger_symbol": bool(hazard.get("contains_danger_symbol")),
                    "danger_symbols": hazard.get("danger_symbols", []),
                    "alignment_token_word_mismatch": mapping_mismatch,
                    "review_reason_codes": sorted(set(review_reasons)),
                    "textgrid": str(textgrid_path.relative_to(run_root)),
                }
            )
    aligned_words_path = resolve_under_root(run_root, "artifacts/aligned_words.jsonl")
    write_jsonl(aligned_words_path, rows)
    reason_counts = Counter(reason for row in rows for reason in row["review_reason_codes"])
    summary = {
        "stage": "mfa",
        "config_hash": str(config.get("config_hash") or ""),
        "input_artifact_hashes": {
            "asr_mfa_queue_json": sha256_file(queue_path),
            "normalized_transcripts_json": sha256_file(normalized_path),
        },
        "output_hashes": {"aligned_words_jsonl": sha256_file(aligned_words_path)},
        "expected_textgrid_count": len(expected_buffer_ids),
        "textgrid_count": len(textgrid_paths),
        "missing_textgrid_count": len(missing_textgrid_buffer_ids),
        "missing_textgrid_buffer_ids": missing_textgrid_buffer_ids,
        "unexpected_textgrid_count": len(unexpected_textgrid_stems),
        "unexpected_textgrid_stems": unexpected_textgrid_stems,
        "aligned_word_count": len(rows),
        "oov_word_count": sum(row["is_oov"] for row in rows),
        "numeric_oov_word_count": sum(row["is_numeric_oov"] for row in rows),
        "words_with_symbol_hazards": sum(row["contains_danger_symbol"] for row in rows),
        "words_with_numeric_hazards": sum(row["contains_numeric"] for row in rows),
        "buffers_with_alignment_token_word_mismatch": len(
            {row["buffer_id"] for row in rows if row["alignment_token_word_mismatch"]}
        ),
        "review_reason_counts": dict(sorted(reason_counts.items())),
        "reason_codes": (
            (["missing_textgrids"] if missing_textgrid_buffer_ids else [])
            + (["unexpected_textgrids"] if unexpected_textgrid_stems else [])
        ),
    }
    write_json(resolve_under_root(run_root, "artifacts/aligned_words_summary.json"), summary)
    return summary


def run_mfa_alignment(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    corpus_summary = build_mfa_corpus(run_root, config)
    if corpus_summary["included_utterances"] == 0:
        aligned_words_path = resolve_under_root(run_root, "artifacts/aligned_words.jsonl")
        write_jsonl(aligned_words_path, [])
        mfa_summary = {
            "stage": "mfa",
            "config_hash": str(config.get("config_hash") or ""),
            "input_artifact_hashes": {
                "mfa_corpus_manifest_json": sha256_file(resolve_under_root(run_root, "artifacts/mfa_corpus_manifest.json")),
            },
            "status": "skipped",
            "returncode": None,
            "command": [],
            "stdout_tail": "",
            "stderr_tail": "",
            "corpus_dir": "artifacts/mfa_corpus",
            "output_dir": "artifacts/mfa_output",
            "dictionary": str(config.get("mfa_dictionary") or DEFAULT_MFA_DICTIONARY),
            "acoustic_model": str(config.get("mfa_acoustic_model") or DEFAULT_MFA_ACOUSTIC_MODEL),
            "single_speaker": bool(config.get("mfa_single_speaker", True)),
            "reason_codes": ["empty_mfa_corpus"],
        }
        mfa_summary_path = resolve_under_root(run_root, "artifacts/mfa_summary.json")
        write_json(mfa_summary_path, mfa_summary)
        summary = {
            "stage": "mfa",
            "config_hash": str(config.get("config_hash") or ""),
            "input_artifact_hashes": corpus_summary["input_artifact_hashes"],
            "output_hashes": {
                "aligned_words_jsonl": sha256_file(aligned_words_path),
                "mfa_summary_json": sha256_file(mfa_summary_path),
            },
            "status": "skipped",
            "expected_textgrid_count": 0,
            "textgrid_count": 0,
            "missing_textgrid_count": 0,
            "missing_textgrid_buffer_ids": [],
            "unexpected_textgrid_count": 0,
            "unexpected_textgrid_stems": [],
            "aligned_word_count": 0,
            "reason_codes": ["empty_mfa_corpus"],
        }
        write_json(resolve_under_root(run_root, "artifacts/aligned_words_summary.json"), summary)
        return summary
    mfa_summary = run_mfa_command(run_root, config)
    aligned_summary = parse_mfa_textgrids(run_root, config)
    return {**aligned_summary, "mfa_command": mfa_summary}
