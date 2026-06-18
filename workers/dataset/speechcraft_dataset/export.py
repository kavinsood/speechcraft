from __future__ import annotations

import shutil
import wave
from collections import Counter
from pathlib import Path
from typing import Any

from .io import read_json_value, resolve_under_root, sha256_file, write_json

QC_SOURCE_DATASET_QC = "artifacts/dataset_qc.json"
QC_SOURCE_CANDIDATE_MANIFEST = "artifacts/candidate_review_manifest.json"
VALID_DATASET_QC_STATUSES = {"accepted", "rejected"}
VALID_MANUAL_OVERRIDES = {None, "force_keep", "force_reject"}


def _load_dataset_qc(run_root: Path) -> dict[str, Any] | None:
    path = resolve_under_root(run_root, QC_SOURCE_DATASET_QC)
    if not path.exists():
        return None
    payload = read_json_value(path)
    if not isinstance(payload, dict):
        raise ValueError("dataset_qc.json must contain an object")
    if payload.get("schema_version") != 1:
        raise ValueError("dataset_qc.json must contain schema_version == 1")
    if payload.get("stage") != "dataset_qc":
        raise ValueError("dataset_qc.json must contain stage == 'dataset_qc'")
    if not isinstance(payload.get("thresholds"), dict):
        raise ValueError("dataset_qc.json must contain thresholds object")
    if not isinstance(payload.get("score_methods"), dict):
        raise ValueError("dataset_qc.json must contain score_methods object")
    clips = payload.get("clips")
    if not isinstance(clips, list):
        raise ValueError("dataset_qc.json must contain clips[]")
    for row in clips:
        if not isinstance(row, dict):
            raise ValueError("dataset_qc.json clips[] row must be an object")
    return payload


def _index_dataset_qc_clips(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in payload.get("clips", []):
        clip_id = row.get("clip_id") or row.get("id")
        if not isinstance(clip_id, str) or not clip_id:
            raise ValueError("dataset_qc.json clips[] row must contain clip_id")
        status = row.get("status")
        if status not in VALID_DATASET_QC_STATUSES:
            raise ValueError(f"dataset_qc.json clip has invalid status for {clip_id}: {status!r}")
        manual_override = row.get("manual_override")
        if manual_override not in VALID_MANUAL_OVERRIDES:
            raise ValueError(
                f"dataset_qc.json clip has invalid manual_override for {clip_id}: {manual_override!r}"
            )
        if clip_id in indexed:
            raise ValueError(f"duplicate clip_id in dataset_qc.json: {clip_id}")
        indexed[clip_id] = row
    return indexed


def _qc_override_counts(payload: dict[str, Any]) -> dict[str, int]:
    counts = {"force_keep": 0, "force_reject": 0}
    for row in payload.get("manual_overrides", []):
        if not isinstance(row, dict):
            raise ValueError("dataset_qc.json manual_overrides[] row must be an object")
        override = row.get("override")
        clip_id = row.get("clip_id")
        if not isinstance(clip_id, str) or not clip_id:
            raise ValueError("dataset_qc.json manual_overrides[] row must contain clip_id")
        if override in counts:
            counts[override] += 1
        else:
            raise ValueError(f"dataset_qc.json manual_overrides[] has invalid override: {override!r}")
    return counts


def _source_maps(run_root: Path) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    source_manifest = read_json_value(resolve_under_root(run_root, "artifacts/source_audio_manifest.json"))
    variant_manifest = read_json_value(resolve_under_root(run_root, "artifacts/audio_variants_manifest.json"))
    sources = {str(row["source_audio_id"]): row for row in source_manifest.get("sources", [])}
    variants = {
        str(row["source_audio_id"]): row
        for row in variant_manifest.get("variants", [])
        if str(row.get("kind")) == "analysis_audio"
    }
    return sources, variants


def _analysis_to_native_sample(analysis_sample: int, variant: dict[str, Any]) -> int:
    analysis_rate = int(variant["analysis_sample_rate"])
    source_rate = int(variant["source_sample_rate"])
    return int(round(analysis_sample * (source_rate / analysis_rate)))


def _slice_native_wav(source_path: Path, output_path: Path, start_frame: int, end_frame: int) -> dict[str, Any]:
    if end_frame <= start_frame:
        raise ValueError(f"Invalid native export bounds: {start_frame}:{end_frame}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(source_path), "rb") as source:
        total_frames = source.getnframes()
        if start_frame < 0 or end_frame > total_frames:
            raise ValueError(f"Native export bounds escape source WAV: {start_frame}:{end_frame} > {total_frames}")
        params = source.getparams()
        source.setpos(start_frame)
        frames = source.readframes(end_frame - start_frame)
    with wave.open(str(output_path), "wb") as output:
        output.setparams(params)
        output.writeframes(frames)
    with wave.open(str(output_path), "rb") as exported:
        return {
            "sample_rate": exported.getframerate(),
            "num_channels": exported.getnchannels(),
            "sample_width_bytes": exported.getsampwidth(),
            "duration_samples": exported.getnframes(),
            "duration_sec": round(exported.getnframes() / exported.getframerate(), 6),
        }


def export_native_candidate_clips(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    manifest_path = resolve_under_root(run_root, "artifacts/candidate_review_manifest.json")
    candidate_manifest = read_json_value(manifest_path)
    if not isinstance(candidate_manifest, list):
        raise ValueError("candidate_review_manifest.json must contain a list")
    sources, variants = _source_maps(run_root)
    dataset_qc = _load_dataset_qc(run_root)
    dataset_qc_by_id = _index_dataset_qc_clips(dataset_qc) if dataset_qc is not None else {}
    qc_source = QC_SOURCE_DATASET_QC if dataset_qc is not None else QC_SOURCE_CANDIDATE_MANIFEST
    qc_thresholds = dataset_qc.get("thresholds") if dataset_qc is not None and isinstance(dataset_qc.get("thresholds"), dict) else None
    qc_score_methods = dataset_qc.get("score_methods") if dataset_qc is not None and isinstance(dataset_qc.get("score_methods"), dict) else None
    qc_override_counts = _qc_override_counts(dataset_qc) if dataset_qc is not None else {"force_keep": 0, "force_reject": 0}

    export_statuses = set(config.get("native_export_statuses") or ["candidate_review", "accepted"])
    export_dir = resolve_under_root(run_root, "artifacts/native_export_clips")
    if export_dir.exists():
        shutil.rmtree(export_dir)
    export_dir.mkdir(parents=True, exist_ok=True)

    exported: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for candidate in candidate_manifest:
        clip_id = str(candidate.get("id") or "")
        manual_override = None
        if dataset_qc is not None:
            qc_row = dataset_qc_by_id.get(clip_id)
            if qc_row is None:
                rejected.append(
                    {
                        "candidate_id": clip_id,
                        "qc_source": qc_source,
                        "reason_codes": ["missing_dataset_qc_clip"],
                    }
                )
                continue
            status = str(qc_row.get("status") or "")
            manual_override = qc_row.get("manual_override")
            if status != "accepted":
                rejected.append(
                    {
                        "candidate_id": clip_id,
                        "qc_source": qc_source,
                        "status": status,
                        "manual_override": manual_override,
                        "reason_codes": ["dataset_qc_status_not_accepted"],
                    }
                )
                continue
        else:
            status = str(candidate.get("review_status") or candidate.get("status") or "")
            if status not in export_statuses:
                rejected.append(
                    {
                        "candidate_id": clip_id,
                        "qc_source": qc_source,
                        "status": status,
                        "reason_codes": ["candidate_status_not_exportable"],
                    }
                )
                continue
        source_audio_id = str(candidate.get("source_audio_id") or "")
        source = sources.get(source_audio_id)
        variant = variants.get(source_audio_id)
        if source is None or variant is None:
            rejected.append(
                {
                    "candidate_id": clip_id,
                    "source_audio_id": source_audio_id,
                    "qc_source": qc_source,
                    "reason_codes": ["missing_source_or_analysis_variant"],
                }
            )
            continue

        analysis_start = int(candidate["source_start_sample"])
        analysis_end = int(candidate["source_end_sample"])
        native_start = _analysis_to_native_sample(analysis_start, variant)
        native_end = _analysis_to_native_sample(analysis_end, variant)
        source_num_samples = int(source["num_samples"])
        native_start = max(0, min(source_num_samples, native_start))
        native_end = max(0, min(source_num_samples, native_end))
        rel_audio_path = f"artifacts/native_export_clips/{clip_id}.wav"
        output_path = resolve_under_root(run_root, rel_audio_path)
        native_info = _slice_native_wav(Path(str(source["path"])), output_path, native_start, native_end)
        expected_duration_sec = (analysis_end - analysis_start) / int(variant["analysis_sample_rate"])
        exported.append(
            {
                "id": clip_id,
                "candidate_id": clip_id,
                "source_audio_id": source_audio_id,
                "audio_path": rel_audio_path,
                "audio_hash": sha256_file(output_path),
                "sample_rate": native_info["sample_rate"],
                "num_channels": native_info["num_channels"],
                "sample_width_bytes": native_info["sample_width_bytes"],
                "analysis_start_sample": analysis_start,
                "analysis_end_sample": analysis_end,
                "native_start_sample": native_start,
                "native_end_sample": native_end,
                "duration_samples": native_info["duration_samples"],
                "duration_sec": native_info["duration_sec"],
                "analysis_duration_sec": round(expected_duration_sec, 6),
                "duration_delta_sec": round(native_info["duration_sec"] - expected_duration_sec, 9),
                "training_text": str(candidate.get("training_text") or ""),
                "alignment_text": str(candidate.get("alignment_text") or ""),
                "needs_review": bool(candidate.get("needs_review")),
                "review_reason_codes": list(candidate.get("review_reason_codes") or []),
                "source_candidate_audio_path": candidate.get("audio_path"),
                "start_cutpoint_ref": candidate.get("start_cutpoint_ref"),
                "end_cutpoint_ref": candidate.get("end_cutpoint_ref"),
                "word_ids": list(candidate.get("word_ids") or []),
                "export_status": "native_exported",
                "qc_source": qc_source,
                "qc_status": "accepted" if dataset_qc is not None else status,
                "manual_override": manual_override,
                "qc_thresholds": dict(qc_thresholds) if qc_thresholds is not None else None,
                "qc_score_methods": dict(qc_score_methods) if qc_score_methods is not None else None,
            }
        )

    export_manifest_path = resolve_under_root(run_root, "artifacts/export_manifest.json")
    export_audit_path = resolve_under_root(run_root, "artifacts/export_audit.json")
    write_json(export_manifest_path, exported)
    write_json(export_audit_path, rejected)
    durations = [float(row["duration_sec"]) for row in exported]
    rejection_reason_counts = Counter(reason for row in rejected for reason in row["reason_codes"])
    input_artifact_hashes = {
        "candidate_review_manifest_json": sha256_file(manifest_path),
        "source_audio_manifest_json": sha256_file(resolve_under_root(run_root, "artifacts/source_audio_manifest.json")),
        "audio_variants_manifest_json": sha256_file(resolve_under_root(run_root, "artifacts/audio_variants_manifest.json")),
    }
    if dataset_qc is not None:
        input_artifact_hashes["dataset_qc_json"] = sha256_file(resolve_under_root(run_root, QC_SOURCE_DATASET_QC))
    summary = {
        "stage": "native_export",
        "config_hash": str(config.get("config_hash") or ""),
        "input_artifact_hashes": input_artifact_hashes,
        "output_hashes": {
            "export_manifest_json": sha256_file(export_manifest_path),
            "export_audit_json": sha256_file(export_audit_path),
            "native_export_wavs": {row["id"]: row["audio_hash"] for row in exported},
        },
        "exported_clip_count": len(exported),
        "rejected_candidate_count": len(rejected),
        "total_duration_sec": round(sum(durations), 6),
        "min_clip_duration_sec": min(durations, default=None),
        "max_clip_duration_sec": max(durations, default=None),
        "sample_rates": sorted({row["sample_rate"] for row in exported}),
        "channel_counts": sorted({row["num_channels"] for row in exported}),
        "duration_delta_abs_max_sec": max((abs(float(row["duration_delta_sec"])) for row in exported), default=0.0),
        "rejection_reason_counts": dict(sorted(rejection_reason_counts.items())),
        "export_statuses": sorted(export_statuses),
        "output_dir": "artifacts/native_export_clips",
        "qc_source": qc_source,
        "qc_thresholds": dict(qc_thresholds) if qc_thresholds is not None else None,
        "qc_score_methods": dict(qc_score_methods) if qc_score_methods is not None else None,
        "manual_override_counts": qc_override_counts,
    }
    write_json(resolve_under_root(run_root, "artifacts/export_summary.json"), summary)
    return summary
