#!/usr/bin/env python3
from __future__ import annotations

import argparse
import audioop
import io
import json
import shutil
import sys
import tempfile
import time
import wave
from collections import defaultdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.app.models import ReferenceRunCreate
from backend.app.repository import SQLiteRepository

LOCKED_MODEL_CHOICES = (
    "acoustic_signature_v1",
    "microsoft/wavlm-base-plus",
    "facebook/wav2vec2-base-960h",
)
DEFAULT_MODEL = "microsoft/wavlm-base-plus"
DEFAULT_PROJECT_ID = "emmawatson-train"
DEFAULT_CANDIDATE_CAP = 200
DEFAULT_TOP_K = 5


def log(step: str, detail: str) -> None:
    print(f"[eval] {step}: {detail}", flush=True)


def cosine(a: list[float], b: list[float]) -> float:
    return float(sum(x * y for x, y in zip(a, b)))


def _slug(text: str, limit: int = 48) -> str:
    import re

    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text[:limit].rstrip("-") or "no-transcript"


def _make_repository(temp_root: Path) -> SQLiteRepository:
    source_db = REPO_ROOT / "backend" / "data" / "project.db"
    media_root = REPO_ROOT / "backend" / "data" / "media"
    temp_db = temp_root / "project.db"
    shutil.copy2(source_db, temp_db)
    return SQLiteRepository(
        db_path=temp_db,
        media_root=media_root,
        exports_root=temp_root / "exports",
    )


def _build_temp_run(
    project_id: str,
    candidate_count_cap: int,
    recording_ids: list[str] | None = None,
) -> tuple[SQLiteRepository, Any, list[Any], Path]:
    temp_root = Path(tempfile.mkdtemp(prefix="speechcraft-reference-eval-"))
    repository = _make_repository(temp_root)
    recordings = repository.list_source_recordings(project_id)
    selected_recording_ids = recording_ids or [recording.id for recording in recordings]

    create_started = time.perf_counter()
    run = repository.create_reference_run(
        project_id,
        ReferenceRunCreate(
            recording_ids=selected_recording_ids,
            candidate_count_cap=candidate_count_cap,
        ),
    )
    created_at = time.perf_counter()
    processed = repository.process_reference_run(run.id)
    processed_at = time.perf_counter()
    candidates = repository.list_reference_run_candidates(processed.id, 0, candidate_count_cap)
    processed._eval_create_seconds = round(created_at - create_started, 3)
    processed._eval_process_seconds = round(processed_at - created_at, 3)
    return repository, processed, candidates, temp_root


def _candidate_catalog_entry(repository: SQLiteRepository, run_id: str, candidate: Any) -> dict[str, Any]:
    preview_path = repository.get_reference_candidate_media_path(run_id, candidate.candidate_id)
    return {
        "candidate_id": candidate.candidate_id,
        "source_recording_id": candidate.source_recording_id,
        "source_start_seconds": candidate.source_start_seconds,
        "source_end_seconds": candidate.source_end_seconds,
        "duration_seconds": candidate.duration_seconds,
        "transcript_text": candidate.transcript_text,
        "speaker_name": candidate.speaker_name,
        "language": candidate.language,
        "default_scores": candidate.default_scores,
        "preview_path": str(preview_path),
    }


def _copy_candidate_previews(
    repository: SQLiteRepository,
    run_id: str,
    candidates: list[Any],
    export_dir: Path,
) -> tuple[list[dict[str, Any]], Path]:
    export_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = export_dir / "manifest.tsv"
    manifest_lines = [
        "index\tfile_name\tcandidate_id\tsource_recording_id\tspan_seconds\ttranscript"
    ]
    exported: list[dict[str, Any]] = []
    for index, candidate in enumerate(candidates, start=1):
        preview_path = repository.get_reference_candidate_media_path(run_id, candidate.candidate_id)
        source_id = (candidate.source_recording_id or "no-source").replace("source-", "")
        start = f"{candidate.source_start_seconds:.2f}".replace(".", "p")
        end = f"{candidate.source_end_seconds:.2f}".replace(".", "p")
        transcript_slug = _slug(candidate.transcript_text or "")
        file_name = f"{index:03d}__{source_id}__{start}s_to_{end}s__{transcript_slug}.wav"
        copied_path = export_dir / file_name
        shutil.copy2(preview_path, copied_path)
        transcript_text = (candidate.transcript_text or "").replace("\n", " ").strip()
        manifest_lines.append(
            "\t".join(
                [
                    f"{index:03d}",
                    file_name,
                    candidate.candidate_id,
                    candidate.source_recording_id or "",
                    f"{candidate.source_start_seconds:.2f}-{candidate.source_end_seconds:.2f}",
                    transcript_text,
                ]
            )
        )
        exported.append(
            {
                "candidate_id": candidate.candidate_id,
                "source_recording_id": candidate.source_recording_id,
                "source_start_seconds": candidate.source_start_seconds,
                "source_end_seconds": candidate.source_end_seconds,
                "duration_seconds": candidate.duration_seconds,
                "transcript_text": candidate.transcript_text,
                "speaker_name": candidate.speaker_name,
                "language": candidate.language,
                "default_scores": candidate.default_scores,
                "preview_path": str(copied_path),
                "preview_file_name": file_name,
            }
        )
    manifest_path.write_text("\n".join(manifest_lines) + "\n")
    readme_path = export_dir / "README.txt"
    readme_path.write_text(
        "Stable listening export for reference-space probe curation.\n\n"
        "This folder contains copied candidate preview WAVs plus manifest.tsv.\n"
        "These files do not depend on the temporary evaluation run remaining on disk.\n"
    )
    return exported, manifest_path


def export_candidate_catalog(
    project_id: str,
    candidate_count_cap: int,
    output_path: Path,
    audio_export_dir: Path | None = None,
    recording_ids: list[str] | None = None,
) -> int:
    repository, run, candidates, temp_root = _build_temp_run(project_id, candidate_count_cap, recording_ids)
    try:
        durable_audio_dir = audio_export_dir or output_path.parent / f"{output_path.stem}_audio"
        exported_candidates, manifest_path = _copy_candidate_previews(
            repository,
            run.id,
            candidates,
            durable_audio_dir,
        )
        payload = {
            "project_id": project_id,
            "run_id": run.id,
            "candidate_count_cap": candidate_count_cap,
            "recording_count": len(recording_ids or repository.list_source_recordings(project_id)),
            "create_run_seconds": getattr(run, "_eval_create_seconds", None),
            "process_run_seconds": getattr(run, "_eval_process_seconds", None),
            "audio_export_dir": str(durable_audio_dir),
            "manifest_path": str(manifest_path),
            "candidates": exported_candidates,
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2))
        log("catalog", f"wrote {len(candidates)} candidate(s) to {output_path}")
        log("audio-export", str(durable_audio_dir))
        return 0
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def _load_hf_embedding_runtime(model_name: str) -> tuple[Any, Any]:
    import torch
    from transformers import AutoFeatureExtractor, AutoModel

    extractor = AutoFeatureExtractor.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)
    model.eval()
    return extractor, model


def _wav_bytes_to_float_mono(audio_bytes: bytes, target_sample_rate: int) -> tuple[Any, int]:
    import numpy as np

    with wave.open(io.BytesIO(audio_bytes), "rb") as wav_file:
        channels = wav_file.getnchannels()
        sample_width = wav_file.getsampwidth()
        sample_rate = wav_file.getframerate()
        frame_count = wav_file.getnframes()
        raw = wav_file.readframes(frame_count)
    if sample_width != 2:
        raise ValueError(f"expected 16-bit PCM WAV, got sample_width={sample_width}")
    if channels > 1:
        raw = audioop.tomono(raw, sample_width, 0.5, 0.5)
        channels = 1
    if sample_rate != target_sample_rate:
        raw, _state = audioop.ratecv(raw, sample_width, channels, sample_rate, target_sample_rate, None)
        sample_rate = target_sample_rate
    audio = np.frombuffer(raw, dtype="<i2").astype("float32") / 32768.0
    return audio, sample_rate


def _embed_audio_bytes_hf(extractor: Any, model: Any, audio_bytes: bytes) -> list[float]:
    import torch

    audio, sample_rate = _wav_bytes_to_float_mono(audio_bytes, extractor.sampling_rate)
    inputs = extractor(audio, sampling_rate=sample_rate, return_tensors="pt")
    with torch.inference_mode():
        outputs = model(**inputs)
        hidden = outputs.last_hidden_state.squeeze(0)
        mean = hidden.mean(dim=0)
        std = hidden.std(dim=0, unbiased=False)
        pooled = torch.cat([mean, std], dim=0)
        pooled = torch.nn.functional.normalize(pooled, dim=0)
    return pooled.cpu().tolist()


def _build_vectors(
    repository: SQLiteRepository,
    run_id: str,
    candidates: list[Any],
    model_name: str,
) -> tuple[str, dict[str, list[float]], float]:
    embed_started = time.perf_counter()
    vectors: dict[str, list[float]] = {}

    if model_name == "acoustic_signature_v1":
        for index, candidate in enumerate(candidates, start=1):
            preview_path = repository.get_reference_candidate_media_path(run_id, candidate.candidate_id)
            vectors[candidate.candidate_id] = repository._normalize_embedding_vector(
                repository._embed_audio_bytes_for_reference_space(preview_path.read_bytes())
            )
            if index % 25 == 0 or index == len(candidates):
                log("embed", f"{index}/{len(candidates)} candidate(s)")
        elapsed = round(time.perf_counter() - embed_started, 3)
        return repository._current_reference_embedding_space_id(), vectors, elapsed

    extractor, model = _load_hf_embedding_runtime(model_name)
    for index, candidate in enumerate(candidates, start=1):
        preview_path = repository.get_reference_candidate_media_path(run_id, candidate.candidate_id)
        vectors[candidate.candidate_id] = _embed_audio_bytes_hf(extractor, model, preview_path.read_bytes())
        if index % 25 == 0 or index == len(candidates):
            log("embed", f"{index}/{len(candidates)} candidate(s)")
    elapsed = round(time.perf_counter() - embed_started, 3)
    return f"hf:{model_name}:mean_std_pool_v1", vectors, elapsed


def _build_vectors_from_preview_entries(
    repository: SQLiteRepository,
    preview_entries: list[dict[str, Any]],
    model_name: str,
) -> tuple[str, dict[str, list[float]], float]:
    embed_started = time.perf_counter()
    vectors: dict[str, list[float]] = {}

    if model_name == "acoustic_signature_v1":
        for index, entry in enumerate(preview_entries, start=1):
            preview_path = Path(entry["preview_path"])
            vectors[entry["candidate_id"]] = repository._normalize_embedding_vector(
                repository._embed_audio_bytes_for_reference_space(preview_path.read_bytes())
            )
            if index % 25 == 0 or index == len(preview_entries):
                log("embed", f"{index}/{len(preview_entries)} candidate(s)")
        elapsed = round(time.perf_counter() - embed_started, 3)
        return repository._current_reference_embedding_space_id(), vectors, elapsed

    extractor, model = _load_hf_embedding_runtime(model_name)
    for index, entry in enumerate(preview_entries, start=1):
        preview_path = Path(entry["preview_path"])
        vectors[entry["candidate_id"]] = _embed_audio_bytes_hf(extractor, model, preview_path.read_bytes())
        if index % 25 == 0 or index == len(preview_entries):
            log("embed", f"{index}/{len(preview_entries)} candidate(s)")
    elapsed = round(time.perf_counter() - embed_started, 3)
    return f"hf:{model_name}:mean_std_pool_v1", vectors, elapsed


def _load_catalog_candidates(catalog_paths: list[Path]) -> tuple[list[dict[str, Any]], list[str]]:
    entries: list[dict[str, Any]] = []
    project_ids: list[str] = []
    seen_candidate_ids: set[str] = set()
    for catalog_path in catalog_paths:
        payload = json.loads(catalog_path.read_text())
        project_id = str(payload.get("project_id") or "").strip()
        if project_id:
            project_ids.append(project_id)
        for candidate in payload.get("candidates", []):
            candidate_id = str(candidate.get("candidate_id") or "").strip()
            if not candidate_id:
                raise ValueError(f"Catalog {catalog_path} contains a candidate with no candidate_id")
            if candidate_id in seen_candidate_ids:
                raise ValueError(f"Duplicate candidate_id across catalogs: {candidate_id}")
            preview_path = Path(str(candidate.get("preview_path") or ""))
            if not preview_path.exists():
                raise ValueError(f"Catalog preview path is missing: {preview_path}")
            entries.append(candidate)
            seen_candidate_ids.add(candidate_id)
    return entries, project_ids


def _evaluate_probe_set(
    candidates: list[Any],
    vectors: dict[str, list[float]],
    probe_payload: dict[str, Any],
) -> dict[str, Any]:
    candidate_ids = {candidate.candidate_id for candidate in candidates}
    top_k = max(1, min(int(probe_payload.get("top_k") or DEFAULT_TOP_K), 25))

    results: list[dict[str, Any]] = []
    for probe in probe_payload.get("probes", []):
        probe_id = str(probe.get("id") or probe.get("anchor_candidate_id") or "unnamed-probe")
        anchor_candidate_id = str(probe.get("anchor_candidate_id") or "").strip()
        if not anchor_candidate_id:
            raise ValueError(f"Probe {probe_id} is missing anchor_candidate_id")
        if anchor_candidate_id not in candidate_ids:
            raise ValueError(f"Probe {probe_id} anchor does not belong to the mined run: {anchor_candidate_id}")
        expected_positive = [
            candidate_id
            for candidate_id in probe.get("expected_positive_candidate_ids", [])
            if candidate_id in candidate_ids and candidate_id != anchor_candidate_id
        ]
        expected_negative = [
            candidate_id
            for candidate_id in probe.get("expected_negative_candidate_ids", [])
            if candidate_id in candidate_ids and candidate_id != anchor_candidate_id
        ]
        ranked = sorted(
            (
                (other_id, cosine(vectors[anchor_candidate_id], other_vector))
                for other_id, other_vector in vectors.items()
                if other_id != anchor_candidate_id
            ),
            key=lambda item: (-item[1], item[0]),
        )
        retrieved = [candidate_id for candidate_id, _score in ranked[:top_k]]
        matched_positive = [candidate_id for candidate_id in retrieved if candidate_id in expected_positive]
        intruded_negative = [candidate_id for candidate_id in retrieved if candidate_id in expected_negative]
        recall = len(matched_positive) / len(expected_positive) if expected_positive else 0.0
        intrusion_rate = len(intruded_negative) / len(expected_negative) if expected_negative else 0.0
        results.append(
            {
                "probe_id": probe_id,
                "label": probe.get("label"),
                "speaker": probe.get("speaker"),
                "style_bucket": probe.get("style_bucket"),
                "anchor_candidate_id": anchor_candidate_id,
                "top_k": top_k,
                "retrieved_candidate_ids": retrieved,
                "matched_positive_candidate_ids": matched_positive,
                "intruded_negative_candidate_ids": intruded_negative,
                "recall_at_k": round(recall, 6),
                "negative_intrusion_rate": round(intrusion_rate, 6),
            }
        )

    average_recall = sum(result["recall_at_k"] for result in results) / len(results) if results else 0.0
    average_intrusion = (
        sum(result["negative_intrusion_rate"] for result in results) / len(results) if results else 0.0
    )
    return {
        "probe_count": len(results),
        "average_recall_at_k": round(average_recall, 6),
        "average_negative_intrusion_rate": round(average_intrusion, 6),
        "probes": results,
    }


def evaluate_reference_space(
    project_id: str,
    candidate_count_cap: int,
    model_name: str,
    probe_set_path: Path,
    output_path: Path | None,
    recording_ids: list[str] | None = None,
) -> int:
    probe_payload = json.loads(probe_set_path.read_text())
    repository, run, candidates, temp_root = _build_temp_run(project_id, candidate_count_cap, recording_ids)
    try:
        log("run", f"{run.id} with {len(candidates)} candidate(s)")
        log("model", model_name)
        space_id, vectors, embedding_seconds = _build_vectors(repository, run.id, candidates, model_name)
        evaluation = _evaluate_probe_set(candidates, vectors, probe_payload)
        payload = {
            "project_id": project_id,
            "run_id": run.id,
            "embedding_space_id": space_id,
            "model": model_name,
            "recording_count": len(recording_ids or repository.list_source_recordings(project_id)),
            "candidate_count": len(candidates),
            "candidate_count_cap": candidate_count_cap,
            "create_run_seconds": getattr(run, "_eval_create_seconds", None),
            "process_run_seconds": getattr(run, "_eval_process_seconds", None),
            "embedding_seconds": embedding_seconds,
            "probe_set_path": str(probe_set_path),
            **evaluation,
        }
        rendered = json.dumps(payload, indent=2)
        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(rendered)
            log("result", f"wrote evaluation to {output_path}")
        print(rendered)
        return 0
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def evaluate_reference_space_from_catalogs(
    catalog_paths: list[Path],
    model_name: str,
    probe_set_path: Path,
    output_path: Path | None,
) -> int:
    temp_root = Path(tempfile.mkdtemp(prefix="speechcraft-reference-eval-mixed-"))
    repository = _make_repository(temp_root)
    try:
        probe_payload = json.loads(probe_set_path.read_text())
        preview_entries, project_ids = _load_catalog_candidates(catalog_paths)
        candidates = [
            type("CatalogCandidate", (), {"candidate_id": entry["candidate_id"]})()
            for entry in preview_entries
        ]
        log("catalogs", ", ".join(str(path) for path in catalog_paths))
        log("model", model_name)
        space_id, vectors, embedding_seconds = _build_vectors_from_preview_entries(
            repository,
            preview_entries,
            model_name,
        )
        evaluation = _evaluate_probe_set(candidates, vectors, probe_payload)
        payload = {
            "project_ids": project_ids,
            "catalog_paths": [str(path) for path in catalog_paths],
            "embedding_space_id": space_id,
            "model": model_name,
            "candidate_count": len(preview_entries),
            "probe_set_path": str(probe_set_path),
            "embedding_seconds": embedding_seconds,
            **evaluation,
        }
        rendered = json.dumps(payload, indent=2)
        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(rendered)
            log("result", f"wrote evaluation to {output_path}")
        print(rendered)
        return 0
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Repeatable reference-space evaluation tooling for mined candidate runs."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    mine = subparsers.add_parser(
        "mine-candidates",
        help="Mine a temporary reference run and export candidate previews/metadata for manual listening.",
    )
    mine.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    mine.add_argument("--candidate-count-cap", type=int, default=DEFAULT_CANDIDATE_CAP)
    mine.add_argument("--recording-id", action="append", default=[])
    mine.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "backend" / "scripts" / "reference_probe_candidates_v1.json",
    )
    mine.add_argument(
        "--audio-export-dir",
        type=Path,
        default=None,
        help="Stable directory to copy preview WAVs into. Defaults to a sibling folder next to --output.",
    )

    evaluate = subparsers.add_parser(
        "evaluate",
        help="Evaluate a locked probe set against a candidate embedding space.",
    )
    evaluate.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    evaluate.add_argument("--candidate-count-cap", type=int, default=DEFAULT_CANDIDATE_CAP)
    evaluate.add_argument("--recording-id", action="append", default=[])
    evaluate.add_argument(
        "--model",
        choices=LOCKED_MODEL_CHOICES,
        default=DEFAULT_MODEL,
        help="Locked model candidates for the next representation pass.",
    )
    evaluate.add_argument(
        "--probe-set",
        type=Path,
        default=REPO_ROOT / "backend" / "scripts" / "reference_probe_set_v1.json",
    )
    evaluate.add_argument("--output", type=Path, default=None)

    evaluate_catalogs = subparsers.add_parser(
        "evaluate-catalogs",
        help="Evaluate a locked probe set against one or more exported candidate catalogs.",
    )
    evaluate_catalogs.add_argument(
        "--catalog",
        type=Path,
        action="append",
        required=True,
        help="Path to a candidate catalog JSON produced by mine-candidates. Pass more than one for a mixed pool.",
    )
    evaluate_catalogs.add_argument(
        "--model",
        choices=LOCKED_MODEL_CHOICES,
        default=DEFAULT_MODEL,
        help="Locked model candidates for the next representation pass.",
    )
    evaluate_catalogs.add_argument(
        "--probe-set",
        type=Path,
        required=True,
    )
    evaluate_catalogs.add_argument("--output", type=Path, default=None)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    recording_ids = [recording_id for recording_id in getattr(args, "recording_id", []) if recording_id]

    if args.command == "mine-candidates":
        return export_candidate_catalog(
            project_id=args.project_id,
            candidate_count_cap=args.candidate_count_cap,
            output_path=args.output,
            audio_export_dir=args.audio_export_dir,
            recording_ids=recording_ids or None,
        )

    if args.command == "evaluate":
        return evaluate_reference_space(
            project_id=args.project_id,
            candidate_count_cap=args.candidate_count_cap,
            model_name=args.model,
            probe_set_path=args.probe_set,
            output_path=args.output,
            recording_ids=recording_ids or None,
        )

    if args.command == "evaluate-catalogs":
        return evaluate_reference_space_from_catalogs(
            catalog_paths=args.catalog,
            model_name=args.model,
            probe_set_path=args.probe_set,
            output_path=args.output,
        )

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
