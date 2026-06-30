from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlmodel import Session, delete, select

from .dataset_worker_client import dataset_worker_python, dataset_worker_root, run_dataset_worker_preflight
from .defaults import build_dataset_worker_config, build_slicer_config_overrides, DATASET_SLICER_HARDCODED, SLICER_UI_CONFIG_KEYS
from .models import (
    DatasetExportResultsView,
    DatasetExportRerunRequest,
    DatasetRunArtifactView,
    DatasetRunCreateRequest,
    DatasetRunResumeRequest,
    DatasetRunLogView,
    DatasetSpeakerResultsView,
    DatasetSpeakerSampleView,
    DatasetSpeakerSelectionUpdateRequest,
    DatasetSpeakerSelectionView,
    DatasetRunView,
    DatasetSlicerResultsView,
    DatasetSlicerRerunRequest,
    ProcessingRun,
    ProcessingRunStatus,
    RfcStage,
    RunArtifact,
    RunArtifactKind,
    RunArtifactStatus,
    SourceRecording,
    resolve_run_artifact_path,
    utc_now,
)


LOG_TAIL_CHARS = 50000
RESERVED_WORKER_CONFIG_KEYS = {
    "config_hash",
    "mode",
    "pipeline_version",
    "source_wavs",
    "target_speaker_label",
}
SLICER_CONFIG_KEYS = set(SLICER_UI_CONFIG_KEYS) | set(DATASET_SLICER_HARDCODED)
WORKER_STAGE_MAP = {
    "source_audio": RfcStage.INGEST,
    "audio_variants": RfcStage.AUDIO_VARIANTS,
    "vad": RfcStage.VAD,
    "diarization": RfcStage.DIARIZATION,
    "buffers": RfcStage.PROCESSING_BUFFERS,
    "asr_queue": RfcStage.PROCESSING_BUFFERS,
    "asr": RfcStage.ASR,
    "normalization": RfcStage.NORMALIZATION,
    "mfa": RfcStage.MFA,
    "alignment_qc": RfcStage.MFA,
    "safe_cutpoints": RfcStage.SAFE_CUTPOINTS,
    "candidate_review_clips": RfcStage.CANDIDATE_CLIPS,
    "transcript_qc": RfcStage.TRANSCRIPT_QC,
    "speaker_purity": RfcStage.SPEAKER_PURITY,
    "native_export": RfcStage.EXPORT,
}
ARTIFACT_KINDS = {
    "config.json": RunArtifactKind.RUN_CONFIG_JSON,
    "status.json": RunArtifactKind.RUN_STATUS_JSON,
    "runtime_versions.json": RunArtifactKind.RUNTIME_VERSIONS_JSON,
    "logs/dataset_worker.log": RunArtifactKind.WORKER_LOG,
    "logs/dataset_worker_process.log": RunArtifactKind.WORKER_PROCESS_LOG,
    "artifacts/source_audio_manifest.json": RunArtifactKind.SOURCE_AUDIO_MANIFEST_JSON,
    "artifacts/source_audio_summary.json": RunArtifactKind.SOURCE_AUDIO_SUMMARY_JSON,
    "artifacts/audio_variants_manifest.json": RunArtifactKind.AUDIO_VARIANTS_MANIFEST_JSON,
    "artifacts/audio_variants_summary.json": RunArtifactKind.AUDIO_VARIANTS_SUMMARY_JSON,
    "artifacts/vad_segments.jsonl": RunArtifactKind.VAD_SEGMENTS_JSONL,
    "artifacts/vad_summary.json": RunArtifactKind.VAD_SUMMARY_JSON,
    "artifacts/speaker_regions.jsonl": RunArtifactKind.SPEAKER_REGIONS_JSONL,
    "artifacts/speaker_regions_summary.json": RunArtifactKind.SPEAKER_REGIONS_SUMMARY_JSON,
    "artifacts/speaker_samples_manifest.json": RunArtifactKind.SPEAKER_SAMPLES_MANIFEST_JSON,
    "artifacts/speaker_selection.json": RunArtifactKind.SPEAKER_SELECTION_JSON,
    "artifacts/processing_buffers.json": RunArtifactKind.PROCESSING_BUFFERS_JSON,
    "artifacts/processing_buffer_summary.json": RunArtifactKind.PROCESSING_BUFFER_SUMMARY_JSON,
    "artifacts/asr_mfa_queue.json": RunArtifactKind.ASR_MFA_QUEUE_JSON,
    "artifacts/asr_mfa_queue_summary.json": RunArtifactKind.ASR_MFA_QUEUE_SUMMARY_JSON,
    "artifacts/rejected_buffers.json": RunArtifactKind.REJECTED_BUFFERS_JSON,
    "artifacts/transcripts.json": RunArtifactKind.ASR_TRANSCRIPTS_JSON,
    "artifacts/transcripts_summary.json": RunArtifactKind.ASR_TRANSCRIPTS_SUMMARY_JSON,
    "artifacts/transcript_hazards.json": RunArtifactKind.TRANSCRIPT_HAZARDS_JSON,
    "artifacts/symbol_hazard_summary.json": RunArtifactKind.SYMBOL_HAZARD_SUMMARY_JSON,
    "artifacts/normalized_transcripts.json": RunArtifactKind.NORMALIZED_TRANSCRIPTS_JSON,
    "artifacts/normalization_summary.json": RunArtifactKind.NORMALIZATION_SUMMARY_JSON,
    "artifacts/mfa_corpus_manifest.json": RunArtifactKind.MFA_CORPUS_MANIFEST_JSON,
    "artifacts/mfa_summary.json": RunArtifactKind.MFA_SUMMARY_JSON,
    "artifacts/oov_words.json": RunArtifactKind.MFA_OOV_WORDS_JSON,
    "artifacts/oov_summary.json": RunArtifactKind.MFA_OOV_SUMMARY_JSON,
    "artifacts/aligned_words.jsonl": RunArtifactKind.ALIGNED_WORDS_JSONL,
    "artifacts/aligned_words_summary.json": RunArtifactKind.ALIGNED_WORDS_SUMMARY_JSON,
    "artifacts/alignment_qc_by_buffer.json": RunArtifactKind.ALIGNMENT_QC_BY_BUFFER_JSON,
    "artifacts/alignment_qc_summary.json": RunArtifactKind.ALIGNMENT_QC_SUMMARY_JSON,
    "artifacts/safe_cutpoints.jsonl": RunArtifactKind.SAFE_CUTPOINTS_JSONL,
    "artifacts/rejected_cutpoint_candidates.jsonl": RunArtifactKind.REJECTED_CUTPOINT_CANDIDATES_JSONL,
    "artifacts/safe_cutpoint_summary.json": RunArtifactKind.SAFE_CUTPOINT_SUMMARY_JSON,
    "artifacts/candidate_review_manifest.json": RunArtifactKind.CANDIDATE_REVIEW_MANIFEST_JSON,
    "artifacts/candidate_review_rejected.json": RunArtifactKind.CANDIDATE_REVIEW_REJECTED_JSON,
    "artifacts/candidate_review_summary.json": RunArtifactKind.CANDIDATE_REVIEW_SUMMARY_JSON,
    "artifacts/transcript_qc.json": RunArtifactKind.TRANSCRIPT_QC_JSON,
    "artifacts/transcript_qc_summary.json": RunArtifactKind.TRANSCRIPT_QC_SUMMARY_JSON,
    "artifacts/target_voiceprint.json": RunArtifactKind.TARGET_VOICEPRINT_JSON,
    "artifacts/speaker_purity.json": RunArtifactKind.SPEAKER_PURITY_JSON,
    "artifacts/speaker_purity_summary.json": RunArtifactKind.SPEAKER_PURITY_SUMMARY_JSON,
    "artifacts/dataset_qc.json": RunArtifactKind.DATASET_QC_JSON,
    "artifacts/dataset_qc_summary.json": RunArtifactKind.DATASET_QC_SUMMARY_JSON,
    "artifacts/clip_lab_state.json": RunArtifactKind.CLIP_LAB_STATE_JSON,
    "artifacts/export_manifest.json": RunArtifactKind.EXPORT_MANIFEST_JSON,
    "artifacts/export_audit.json": RunArtifactKind.EXPORT_AUDIT_JSON,
    "artifacts/export_summary.json": RunArtifactKind.EXPORT_SUMMARY_JSON,
}
SUMMARY_ARTIFACT_KINDS = {
    RunArtifactKind.RUN_STATUS_JSON,
    RunArtifactKind.RUNTIME_VERSIONS_JSON,
    RunArtifactKind.SOURCE_AUDIO_SUMMARY_JSON,
    RunArtifactKind.AUDIO_VARIANTS_SUMMARY_JSON,
    RunArtifactKind.VAD_SUMMARY_JSON,
    RunArtifactKind.SPEAKER_REGIONS_SUMMARY_JSON,
    RunArtifactKind.PROCESSING_BUFFER_SUMMARY_JSON,
    RunArtifactKind.ASR_MFA_QUEUE_SUMMARY_JSON,
    RunArtifactKind.ASR_TRANSCRIPTS_SUMMARY_JSON,
    RunArtifactKind.SYMBOL_HAZARD_SUMMARY_JSON,
    RunArtifactKind.NORMALIZATION_SUMMARY_JSON,
    RunArtifactKind.MFA_SUMMARY_JSON,
    RunArtifactKind.MFA_OOV_SUMMARY_JSON,
    RunArtifactKind.ALIGNED_WORDS_SUMMARY_JSON,
    RunArtifactKind.ALIGNMENT_QC_SUMMARY_JSON,
    RunArtifactKind.SAFE_CUTPOINT_SUMMARY_JSON,
    RunArtifactKind.CANDIDATE_REVIEW_SUMMARY_JSON,
    RunArtifactKind.TRANSCRIPT_QC_SUMMARY_JSON,
    RunArtifactKind.SPEAKER_PURITY_SUMMARY_JSON,
    RunArtifactKind.DATASET_QC_SUMMARY_JSON,
    RunArtifactKind.EXPORT_SUMMARY_JSON,
}
SPEAKER_SELECTION_REQUIRED = "speaker_selection_required"
DEFAULT_SINGLE_SPEAKER_ID = "speaker_0"


def dataset_storage_root(repository: Any) -> Path:
    return repository.media_root.resolve()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_json_list(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    return [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []


def _run_root(repository: Any, run: ProcessingRun) -> Path:
    if not run.artifact_root:
        raise ValueError("Dataset run has no artifact root")
    return resolve_run_artifact_path(dataset_storage_root(repository), run.artifact_root, "status.json").parent


def _speaker_selection_path(repository: Any, run: ProcessingRun) -> Path:
    return _run_root(repository, run) / "artifacts" / "speaker_selection.json"


def _read_speaker_selection(repository: Any, run: ProcessingRun) -> dict[str, Any]:
    path = _speaker_selection_path(repository, run)
    return _read_json_object(path) if path.exists() else {}


def _build_worker_command(run_root: Path, run: ProcessingRun, *, stop_after: str) -> list[str]:
    worker_python = dataset_worker_python()
    if not worker_python.exists():
        raise ValueError(f"Dataset worker python not found: {worker_python}")
    command = [
        str(worker_python),
        "-m",
        "speechcraft_dataset.run",
        "--run-root",
        str(run_root),
        "--config",
        str(run_root / "config.json"),
        "--target-speaker-label",
        str(run.input_summary.get("target_speaker_label") or DEFAULT_SINGLE_SPEAKER_ID),
        "--stop-after",
        stop_after,
    ]
    if bool(run.input_summary.get("single_speaker", True)):
        command.append("--single-speaker")
    for source_wav in run.input_summary.get("source_wavs") or []:
        command.extend(["--source-wav", str(source_wav)])
    return command


def _launch_worker(repository: Any, run: ProcessingRun, *, stop_after: str) -> int:
    run_root = _run_root(repository, run)
    command = _build_worker_command(run_root, run, stop_after=stop_after)
    log_path = run_root / "logs" / "dataset_worker_process.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["PYTHONPATH"] = str(dataset_worker_root())
    with log_path.open("ab") as log_handle:
        process = subprocess.Popen(
            command,
            cwd=str(dataset_worker_root()),
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    run.status = ProcessingRunStatus.RUNNING
    run.started_at = utc_now()
    run.completed_at = None
    run.reason_codes = []
    run.input_summary = {
        **run.input_summary,
        "worker_pid": process.pid,
        "worker_command": command,
        "active_stop_after": stop_after,
    }
    return process.pid


def _stop_after_requires_asr(stop_after: str) -> bool:
    return stop_after not in {"diarization", "buffers"}


def _assert_asr_model_available(repository: Any, run: ProcessingRun) -> None:
    config = _read_json_object(_run_root(repository, run) / "config.json")
    preflight = run_dataset_worker_preflight(
        artifact_root=str(run.artifact_root),
        asr_model=str(config.get("faster_whisper_model") or "small.en"),
        asr_model_path=str(config.get("faster_whisper_model_path") or "") or None,
        asr_cache_dir=str(config.get("faster_whisper_cache_dir") or "") or None,
        asr_device=str(config.get("faster_whisper_device") or "cpu"),
        asr_compute_type=str(config.get("faster_whisper_compute_type") or "int8"),
    )
    asr_model = preflight.get("asr_model")
    if isinstance(asr_model, dict) and asr_model.get("ok") is True:
        return
    message = None
    if isinstance(asr_model, dict):
        message = asr_model.get("error")
    if not message:
        message = preflight.get("error")
    raise ValueError(str(message or "ASR model preflight failed"))


def _prepare_run_config_for_launch(repository: Any, run: ProcessingRun, *, stop_after: str) -> None:
    config_path = _run_root(repository, run) / "config.json"
    config = _read_json_object(config_path)
    target_speaker_label = str(run.input_summary.get("target_speaker_label") or DEFAULT_SINGLE_SPEAKER_ID)
    config["target_speaker_label"] = target_speaker_label
    if bool(run.input_summary.get("single_speaker", True)):
        config["mode"] = "single_speaker"
    elif stop_after == "diarization":
        config["mode"] = "diarization"
    else:
        config["mode"] = "selected_speaker"
    config_path.write_text(json.dumps(config, indent=2, sort_keys=True), encoding="utf-8")


def _view(session: Session, run: ProcessingRun) -> DatasetRunView:
    artifacts = list(session.exec(select(RunArtifact).where(RunArtifact.run_id == run.id)))
    return DatasetRunView(
        **run.model_dump(),
        artifacts=[DatasetRunArtifactView(**artifact.model_dump()) for artifact in artifacts],
    )


def create_dataset_run(repository: Any, project_id: str, request: DatasetRunCreateRequest) -> DatasetRunView:
    repository.get_project(project_id)
    with Session(repository.engine, expire_on_commit=False) as session:
        recordings = list(session.exec(select(SourceRecording).where(SourceRecording.batch_id == project_id)))
        by_id = {recording.id: recording for recording in recordings}
        selected_ids = request.source_recording_ids or list(by_id)
        missing = sorted(set(selected_ids) - set(by_id))
        if missing:
            raise KeyError(f"Source recordings not found: {', '.join(missing)}")
        if not selected_ids:
            raise ValueError("Dataset run requires at least one source recording")
        if not request.single_speaker and len(selected_ids) != 1:
            raise ValueError("Multi-speaker diarization currently supports exactly one source WAV per run")
        reserved = sorted(RESERVED_WORKER_CONFIG_KEYS.intersection(request.config))
        if reserved:
            raise ValueError(f"Dataset run config contains backend-controlled keys: {', '.join(reserved)}")

        run_id = f"dataset-{uuid4().hex[:12]}"
        artifact_root = f"dataset-runs/{project_id}/{run_id}"
        run_root = dataset_storage_root(repository) / artifact_root
        run_root.mkdir(parents=True, exist_ok=False)
        config_path = run_root / "config.json"
        worker_config = build_dataset_worker_config(
            language=request.language,
            whisper_model_size=request.whisper_model_size,
            overrides=request.config,
        )
        config_path.write_text(json.dumps(worker_config, indent=2, sort_keys=True), encoding="utf-8")
        run = ProcessingRun(
            id=run_id,
            project_id=project_id,
            artifact_root=artifact_root,
            stage=RfcStage.INGEST,
            status=ProcessingRunStatus.PENDING,
            input_summary={
                "source_recording_ids": selected_ids,
                "source_wavs": [by_id[recording_id].file_path for recording_id in selected_ids],
                "single_speaker": request.single_speaker,
                "target_speaker_label": request.target_speaker_label,
                "requested_stop_after": request.stop_after,
            },
        )
        session.add(run)
        session.commit()
        _index_artifacts(session, repository, run)
        session.commit()
        return _view(session, run)


def start_dataset_run(repository: Any, run_id: str) -> DatasetRunView:
    with Session(repository.engine, expire_on_commit=False) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        if run.status != ProcessingRunStatus.PENDING:
            raise ValueError(f"Only pending dataset runs can start; current status is {run.status.value}")
        requested_stop_after = str(run.input_summary.get("requested_stop_after") or "alignment_qc")
        stop_after = requested_stop_after
        if not bool(run.input_summary.get("single_speaker", True)):
            selection = _read_speaker_selection(repository, run)
            if not bool(selection.get("selected")):
                stop_after = "diarization"
        if _stop_after_requires_asr(stop_after):
            _assert_asr_model_available(repository, run)
        _prepare_run_config_for_launch(repository, run, stop_after=stop_after)
        _launch_worker(repository, run, stop_after=stop_after)
        session.add(run)
        session.commit()
        return _view(session, run)


def refresh_dataset_run(repository: Any, run_id: str) -> DatasetRunView:
    with Session(repository.engine, expire_on_commit=False) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        before = _run_state_snapshot(run)
        status = _read_json_object(_run_root(repository, run) / "status.json")
        pid_alive = _pid_is_running(run.input_summary.get("worker_pid"))
        if status:
            _apply_worker_status_to_run(run, status, pid_alive=pid_alive)
        elif run.status == ProcessingRunStatus.RUNNING and not pid_alive:
            run.status = ProcessingRunStatus.FAILED
            run.reason_codes = ["dataset_worker_exited_without_status"]
            run.completed_at = utc_now()
        artifacts_changed = _index_artifacts(session, repository, run)
        if _run_state_snapshot(run) != before or artifacts_changed:
            session.add(run)
            session.commit()
        return _view(session, run)


def list_dataset_runs(repository: Any, project_id: str) -> list[DatasetRunView]:
    repository.get_project(project_id)
    with Session(repository.engine) as session:
        runs = list(session.exec(select(ProcessingRun).where(ProcessingRun.project_id == project_id)))
        return [_view(session, run) for run in runs]


def get_dataset_run(repository: Any, run_id: str) -> DatasetRunView:
    with Session(repository.engine) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        return _view(session, run)


def get_dataset_run_log(repository: Any, run_id: str) -> DatasetRunLogView:
    with Session(repository.engine) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        relative = "logs/dataset_worker.log"
        path = _run_root(repository, run) / relative
        if not path.exists():
            relative = "logs/dataset_worker_process.log"
            path = _run_root(repository, run) / relative
        text = path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""
        return DatasetRunLogView(run_id=run_id, path=relative, text=text[-LOG_TAIL_CHARS:], truncated=len(text) > LOG_TAIL_CHARS)


def get_dataset_speaker_results(repository: Any, run_id: str) -> DatasetSpeakerResultsView:
    with Session(repository.engine) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        root = _run_root(repository, run)
        summary = _read_json_object(root / "artifacts/speaker_regions_summary.json")
        samples = _read_json_list(root / "artifacts/speaker_samples_manifest.json")
        selection_payload = _read_json_object(root / "artifacts/speaker_selection.json")
        selection = DatasetSpeakerSelectionView(**selection_payload) if selection_payload else None
        return DatasetSpeakerResultsView(
            run_id=run_id,
            speaker_regions_summary=summary,
            speaker_samples_manifest=[DatasetSpeakerSampleView(**row) for row in samples],
            speaker_selection=selection,
        )


def save_dataset_speaker_selection(
    repository: Any,
    run_id: str,
    request: DatasetSpeakerSelectionUpdateRequest,
) -> DatasetSpeakerSelectionView:
    with Session(repository.engine, expire_on_commit=False) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        if bool(run.input_summary.get("single_speaker", True)):
            raise ValueError("Single-speaker runs do not require manual speaker selection")
        results = get_dataset_speaker_results(repository, run_id)
        available_ids = sorted({sample.speaker_id for sample in results.speaker_samples_manifest} | set(results.speaker_regions_summary.get("speaker_ids") or []))
        if request.target_speaker_id not in available_ids:
            raise ValueError(f"Unknown speaker_id: {request.target_speaker_id}")
        payload = {
            "mode": "diarization",
            "selected": True,
            "target_speaker_id": request.target_speaker_id,
            "source": "user",
            "available_speaker_ids": available_ids,
            "updated_at": utc_now().isoformat(),
        }
        path = _speaker_selection_path(repository, run)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        run.input_summary = {**run.input_summary, "target_speaker_label": request.target_speaker_id}
        _index_artifacts(session, repository, run)
        session.add(run)
        session.commit()
        return DatasetSpeakerSelectionView(**payload)


def resume_dataset_run_processing(
    repository: Any,
    run_id: str,
    request: DatasetRunResumeRequest | None = None,
) -> DatasetRunView:
    with Session(repository.engine, expire_on_commit=False) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        if bool(run.input_summary.get("single_speaker", True)):
            raise ValueError("Single-speaker runs do not need speaker-selection resume")
        if run.status == ProcessingRunStatus.RUNNING:
            raise ValueError("Dataset run is already running")
        selection = _read_speaker_selection(repository, run)
        if not bool(selection.get("selected")):
            raise ValueError("speaker_selection.json does not select a target speaker")
        requested_stop_after = str(
            (request.stop_after if request is not None else None)
            or run.input_summary.get("requested_stop_after")
            or "alignment_qc"
        )
        run.input_summary = {**run.input_summary, "requested_stop_after": requested_stop_after}
        if _stop_after_requires_asr(requested_stop_after):
            _assert_asr_model_available(repository, run)
        _prepare_run_config_for_launch(repository, run, stop_after=requested_stop_after)
        _launch_worker(repository, run, stop_after=requested_stop_after)
        session.add(run)
        session.commit()
        return _view(session, run)


def rerun_dataset_slicer(repository: Any, run_id: str, request: DatasetSlicerRerunRequest) -> DatasetRunView:
    unknown = sorted(set(request.config) - SLICER_UI_CONFIG_KEYS)
    if unknown:
        raise ValueError(f"Unsupported slicer config keys: {', '.join(unknown)}")
    with Session(repository.engine, expire_on_commit=False) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        if run.status == ProcessingRunStatus.RUNNING:
            raise ValueError("Dataset run is already running")
        run_root = _run_root(repository, run)
        required = ("artifacts/asr_mfa_queue.json", "artifacts/aligned_words.jsonl", "artifacts/alignment_qc_by_buffer.json")
        missing = [path for path in required if not (run_root / path).exists()]
        if missing:
            raise ValueError(f"Dataset run is not slicer-ready; missing: {', '.join(missing)}")
        from .clip_lab_state import ClipLabStateBusyError, assert_clip_lab_run_available

        try:
            assert_clip_lab_run_available(run_root)
        except ClipLabStateBusyError as exc:
            raise ValueError(str(exc)) from exc
        worker_python = dataset_worker_python()
        if not worker_python.exists():
            raise ValueError(f"Dataset worker python not found: {worker_python}")
        config_path = run_root / "config.json"
        config = _read_json_object(config_path)
        config.update(build_slicer_config_overrides(request.config))
        config_path.write_text(json.dumps(config, indent=2, sort_keys=True), encoding="utf-8")
        status_path = run_root / "status.json"
        status_path.write_text(
            json.dumps(
                {
                    "ok": None,
                    "stage": "safe_cutpoints",
                    "started_at": utc_now().isoformat(),
                    "completed_at": None,
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        command = [str(worker_python), "-m", "speechcraft_dataset.rerun_slicer", "--run-root", str(run_root), "--config", str(config_path)]
        log_path = run_root / "logs" / "dataset_worker_process.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env["PYTHONPATH"] = str(dataset_worker_root())
        with log_path.open("ab") as log_handle:
            process = subprocess.Popen(command, cwd=str(dataset_worker_root()), env=env, stdout=log_handle, stderr=subprocess.STDOUT, start_new_session=True)
        run.status = ProcessingRunStatus.RUNNING
        run.stage = RfcStage.SAFE_CUTPOINTS
        run.started_at = utc_now()
        run.completed_at = None
        run.reason_codes = []
        run.input_summary = {**run.input_summary, "worker_pid": process.pid, "worker_command": command}
        session.add(run)
        session.commit()
        return _view(session, run)


def rerun_dataset_native_export(repository: Any, run_id: str, request: DatasetExportRerunRequest) -> DatasetRunView:
    with Session(repository.engine, expire_on_commit=False) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        if run.status == ProcessingRunStatus.RUNNING:
            raise ValueError("Dataset run is already running")
        run_root = _run_root(repository, run)
        required = (
            "artifacts/candidate_review_manifest.json",
            "artifacts/source_audio_manifest.json",
            "artifacts/audio_variants_manifest.json",
        )
        missing = [path for path in required if not (run_root / path).exists()]
        if missing:
            raise ValueError(f"Dataset run is not export-ready; missing: {', '.join(missing)}")
        worker_python = dataset_worker_python()
        if not worker_python.exists():
            raise ValueError(f"Dataset worker python not found: {worker_python}")
        config_path = run_root / "config.json"
        config = _read_json_object(config_path)
        config.update(request.config)
        config_path.write_text(json.dumps(config, indent=2, sort_keys=True), encoding="utf-8")
        status_path = run_root / "status.json"
        status_path.write_text(
            json.dumps(
                {
                    "ok": None,
                    "stage": "native_export",
                    "started_at": utc_now().isoformat(),
                    "completed_at": None,
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        command = [str(worker_python), "-m", "speechcraft_dataset.rerun_export", "--run-root", str(run_root), "--config", str(config_path)]
        log_path = run_root / "logs" / "dataset_worker_process.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env["PYTHONPATH"] = str(dataset_worker_root())
        with log_path.open("ab") as log_handle:
            process = subprocess.Popen(command, cwd=str(dataset_worker_root()), env=env, stdout=log_handle, stderr=subprocess.STDOUT, start_new_session=True)
        run.status = ProcessingRunStatus.RUNNING
        run.stage = RfcStage.EXPORT
        run.started_at = utc_now()
        run.completed_at = None
        run.reason_codes = []
        run.input_summary = {**run.input_summary, "worker_pid": process.pid, "worker_command": command}
        session.add(run)
        session.commit()
        return _view(session, run)


def generate_dataset_qc_scores(repository: Any, run_id: str, *, force: bool = False) -> DatasetRunView:
    with Session(repository.engine, expire_on_commit=False) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        if run.status == ProcessingRunStatus.RUNNING:
            raise ValueError("Dataset run is already running")
        run_root = _run_root(repository, run)
        if (run_root / "artifacts/dataset_qc.json").exists() and not force:
            raise ValueError("dataset_qc_already_finalized")
        required = (
            "artifacts/candidate_review_manifest.json",
            "artifacts/speaker_selection.json",
            "artifacts/speaker_regions.jsonl",
            "artifacts/audio_variants_manifest.json",
        )
        missing = [path for path in required if not (run_root / path).exists()]
        if missing:
            raise ValueError(f"Dataset run is not QC-score-ready; missing: {', '.join(missing)}")
        worker_python = dataset_worker_python()
        if not worker_python.exists():
            raise ValueError(f"Dataset worker python not found: {worker_python}")
        config_path = run_root / "config.json"
        config = _read_json_object(config_path)
        config_path.write_text(json.dumps(config, indent=2, sort_keys=True), encoding="utf-8")
        status_path = run_root / "status.json"
        status_path.write_text(
            json.dumps(
                {
                    "ok": None,
                    "stage": "transcript_qc",
                    "started_at": utc_now().isoformat(),
                    "completed_at": None,
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        command = [str(worker_python), "-m", "speechcraft_dataset.generate_qc_scores", "--run-root", str(run_root), "--config", str(config_path)]
        if force:
            command.append("--force")
        log_path = run_root / "logs" / "dataset_worker_process.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env["PYTHONPATH"] = str(dataset_worker_root())
        with log_path.open("ab") as log_handle:
            process = subprocess.Popen(command, cwd=str(dataset_worker_root()), env=env, stdout=log_handle, stderr=subprocess.STDOUT, start_new_session=True)
        run.status = ProcessingRunStatus.RUNNING
        run.stage = RfcStage.TRANSCRIPT_QC
        run.started_at = utc_now()
        run.completed_at = None
        run.reason_codes = []
        run.input_summary = {**run.input_summary, "worker_pid": process.pid, "worker_command": command}
        session.add(run)
        session.commit()
        return _view(session, run)


def _read_dataset_slicer_results_from_root(run_id: str, root: Path) -> DatasetSlicerResultsView:
    return DatasetSlicerResultsView(
        run_id=run_id,
        safe_cutpoint_summary=_read_json_object(root / "artifacts/safe_cutpoint_summary.json"),
        candidate_review_summary=_read_json_object(root / "artifacts/candidate_review_summary.json"),
        candidate_review_manifest=_read_json_list(root / "artifacts/candidate_review_manifest.json"),
        candidate_review_rejected=_read_json_list(root / "artifacts/candidate_review_rejected.json"),
        alignment_qc_by_buffer=_read_json_list(root / "artifacts/alignment_qc_by_buffer.json"),
        transcripts=_read_json_list(root / "artifacts/transcripts.json"),
        aligned_words=_read_json_list(root / "artifacts/aligned_words.jsonl"),
    )


def get_dataset_slicer_results(repository: Any, run_id: str) -> DatasetSlicerResultsView:
    from .clip_lab_state import clip_lab_run_lock

    with Session(repository.engine) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        root = _run_root(repository, run)
    with clip_lab_run_lock(root):
        return _read_dataset_slicer_results_from_root(run_id, root)


def get_dataset_export_results(repository: Any, run_id: str) -> DatasetExportResultsView:
    with Session(repository.engine) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        root = _run_root(repository, run)
        return DatasetExportResultsView(
            run_id=run_id,
            export_summary=_read_json_object(root / "artifacts/export_summary.json"),
            export_manifest=_read_json_list(root / "artifacts/export_manifest.json"),
            export_audit=_read_json_list(root / "artifacts/export_audit.json"),
        )


def _resolve_candidate_review_media(
    repository: Any,
    run_id: str,
    clip_id: str,
) -> tuple[Path, bytes]:
    from .clip_lab_state import clip_lab_run_lock

    with Session(repository.engine) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        root = _run_root(repository, run)
        artifact_root = run.artifact_root
    with clip_lab_run_lock(root):
        results = _read_dataset_slicer_results_from_root(run_id, root)
        clip = next(
            (
                row
                for row in results.candidate_review_manifest
                if str(row.get("id") or row.get("clip_id") or "") == clip_id
            ),
            None,
        )
        if clip is None:
            raise KeyError("Candidate review clip not found")
        if artifact_root is None:
            raise KeyError("Candidate review audio not found")
        path = resolve_run_artifact_path(
            dataset_storage_root(repository),
            artifact_root,
            str(clip["audio_path"]),
        )
        if not path.exists() or not path.is_file():
            raise KeyError("Candidate review audio not found")
        return path, path.read_bytes()


def get_candidate_review_media_bytes(repository: Any, run_id: str, clip_id: str) -> bytes:
    _, audio_bytes = _resolve_candidate_review_media(repository, run_id, clip_id)
    return audio_bytes


def get_candidate_review_media_path(repository: Any, run_id: str, clip_id: str) -> Path:
    path, _ = _resolve_candidate_review_media(repository, run_id, clip_id)
    return path


def get_native_export_media_path(repository: Any, run_id: str, clip_id: str) -> Path:
    results = get_dataset_export_results(repository, run_id)
    clip = next((row for row in results.export_manifest if str(row.get("id")) == clip_id), None)
    if clip is None:
        raise KeyError("Native export clip not found")
    with Session(repository.engine) as session:
        run = session.get(ProcessingRun, run_id)
        assert run is not None and run.artifact_root is not None
        path = resolve_run_artifact_path(dataset_storage_root(repository), run.artifact_root, str(clip["audio_path"]))
    if not path.exists() or not path.is_file():
        raise KeyError("Native export audio not found")
    return path


def get_speaker_sample_media_path(repository: Any, run_id: str, sample_id: str) -> Path:
    results = get_dataset_speaker_results(repository, run_id)
    sample = next((row for row in results.speaker_samples_manifest if row.sample_id == sample_id), None)
    if sample is None:
        raise KeyError("Speaker sample not found")
    with Session(repository.engine) as session:
        run = session.get(ProcessingRun, run_id)
        assert run is not None and run.artifact_root is not None
        path = resolve_run_artifact_path(dataset_storage_root(repository), run.artifact_root, sample.audio_path)
    if not path.exists() or not path.is_file():
        raise KeyError("Speaker sample audio not found")
    return path


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _worker_status_in_progress(status: dict[str, Any]) -> bool:
    if not status or status.get("completed_at"):
        return False
    if status.get("ok") is True or status.get("ok") is False:
        return False
    stage = str(status.get("stage") or "")
    return stage not in {"", "starting"}


def _apply_worker_status_to_run(
    run: ProcessingRun,
    status: dict[str, Any],
    *,
    pid_alive: bool,
) -> None:
    stage_name = str(status.get("stage") or "")
    mapped_stage = WORKER_STAGE_MAP.get(stage_name)
    if mapped_stage is not None:
        run.stage = mapped_stage

    run.output_summary = dict(status.get("summary") or {})
    run.reason_codes = list(status.get("reason_codes") or [])
    if mapped_stage is None and stage_name not in {"", "starting"}:
        if "unknown_worker_stage" not in run.reason_codes:
            run.reason_codes.append("unknown_worker_stage")
    run.config_hash = status.get("config_hash") or run.config_hash

    completed_at = status.get("completed_at")
    ok = status.get("ok")

    if ok is True and not completed_at:
        run.status = ProcessingRunStatus.FAILED
        run.reason_codes = ["dataset_worker_malformed_status"]
        run.completed_at = utc_now()
        return

    if ok is True and completed_at:
        parsed_completed_at = _parse_datetime(completed_at)
        if parsed_completed_at is None:
            run.status = ProcessingRunStatus.FAILED
            run.reason_codes = ["dataset_worker_malformed_status"]
            run.completed_at = utc_now()
            return
        run.status = ProcessingRunStatus.COMPLETED
        run.completed_at = parsed_completed_at
        return

    if ok is False and completed_at:
        parsed_completed_at = _parse_datetime(completed_at)
        if parsed_completed_at is None:
            run.status = ProcessingRunStatus.FAILED
            run.reason_codes = ["dataset_worker_malformed_status"]
            run.completed_at = utc_now()
            return
        run.status = ProcessingRunStatus.FAILED
        run.completed_at = parsed_completed_at
        return

    if ok is False and not completed_at:
        run.status = ProcessingRunStatus.FAILED
        run.reason_codes = (
            ["dataset_worker_malformed_status"]
            if pid_alive
            else ["dataset_worker_exited_before_terminal_status"]
        )
        run.completed_at = utc_now()
        return

    if _worker_status_in_progress(status):
        if pid_alive or run.status == ProcessingRunStatus.PENDING:
            run.status = ProcessingRunStatus.RUNNING
            return
        run.status = ProcessingRunStatus.FAILED
        run.reason_codes = ["dataset_worker_exited_before_terminal_status"]
        run.completed_at = utc_now()
        return

    if run.status == ProcessingRunStatus.RUNNING and not pid_alive:
        run.status = ProcessingRunStatus.FAILED
        run.reason_codes = ["dataset_worker_exited_before_terminal_status"]
        run.completed_at = utc_now()
        return

    if pid_alive:
        run.status = ProcessingRunStatus.RUNNING


def _pid_is_running(raw_pid: Any) -> bool:
    try:
        pid = int(raw_pid)
        if pid <= 0:
            return False
        os.kill(pid, 0)
    except (TypeError, ValueError, ProcessLookupError):
        return False
    except PermissionError:
        return True
    return True


def _run_state_snapshot(run: ProcessingRun) -> dict[str, Any]:
    return {
        "stage": run.stage.value,
        "status": run.status.value,
        "config_hash": run.config_hash,
        "input_summary": dict(run.input_summary),
        "output_summary": dict(run.output_summary),
        "reason_codes": list(run.reason_codes),
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
    }


def _artifact_snapshot(artifact: RunArtifact) -> dict[str, Any]:
    return {
        "project_id": artifact.project_id,
        "kind": artifact.kind.value,
        "path": artifact.path,
        "byte_size": artifact.byte_size,
        "content_hash": artifact.content_hash,
        "config_hash": artifact.config_hash,
        "input_artifact_hashes": dict(artifact.input_artifact_hashes),
        "backend": artifact.backend,
        "status": artifact.status.value,
        "summary": dict(artifact.summary),
        "reason_codes": list(artifact.reason_codes),
    }


def _artifact_payload(
    *,
    run: ProcessingRun,
    kind: RunArtifactKind,
    relative_path: str,
    path: Path,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "project_id": run.project_id,
        "kind": kind,
        "path": relative_path,
        "byte_size": path.stat().st_size,
        "content_hash": _sha256(path),
        "config_hash": payload.get("config_hash") or run.config_hash,
        "input_artifact_hashes": dict(payload.get("input_artifact_hashes") or {}),
        "backend": "dataset_worker",
        "status": RunArtifactStatus.MATERIALIZED,
        "summary": payload if kind in SUMMARY_ARTIFACT_KINDS else {},
        "reason_codes": list(payload.get("reason_codes") or []),
    }


def _index_artifacts(session: Session, repository: Any, run: ProcessingRun) -> bool:
    run_root = _run_root(repository, run)
    existing = {
        artifact.id: artifact
        for artifact in session.exec(select(RunArtifact).where(RunArtifact.run_id == run.id))
    }
    seen_ids: set[str] = set()
    changed = False
    for relative_path, kind in ARTIFACT_KINDS.items():
        path = run_root / relative_path
        if not path.exists() or not path.is_file():
            continue
        payload = _read_json_object(path) if path.suffix == ".json" else {}
        if kind == RunArtifactKind.RUN_CONFIG_JSON and payload.get("config_hash"):
            run.config_hash = str(payload["config_hash"])
        artifact_id = f"{run.id}:{kind.value}"
        next_payload = _artifact_payload(
            run=run,
            kind=kind,
            relative_path=relative_path,
            path=path,
            payload=payload,
        )
        seen_ids.add(artifact_id)
        current = existing.get(artifact_id)
        if current is None:
            changed = True
            session.add(
                RunArtifact(
                    id=artifact_id,
                    run_id=run.id,
                    **next_payload,
                )
            )
            continue
        if _artifact_snapshot(current) != {
            **next_payload,
            "kind": next_payload["kind"].value,
            "status": next_payload["status"].value,
        }:
            changed = True
            current.project_id = next_payload["project_id"]
            current.kind = next_payload["kind"]
            current.path = next_payload["path"]
            current.byte_size = next_payload["byte_size"]
            current.content_hash = next_payload["content_hash"]
            current.config_hash = next_payload["config_hash"]
            current.input_artifact_hashes = next_payload["input_artifact_hashes"]
            current.backend = next_payload["backend"]
            current.status = next_payload["status"]
            current.summary = next_payload["summary"]
            current.reason_codes = next_payload["reason_codes"]
            session.add(current)
    stale_ids = set(existing) - seen_ids
    if stale_ids:
        changed = True
        session.exec(delete(RunArtifact).where(RunArtifact.id.in_(stale_ids)))
    return changed
