from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any, Literal

from sqlalchemy import Enum as SQLEnum
from sqlalchemy.orm import validates
from pydantic import field_validator
from sqlmodel import Column, Field, JSON, Relationship, SQLModel


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def sql_enum(enum_cls: type[Enum]) -> SQLEnum:
    return SQLEnum(
        enum_cls,
        values_callable=lambda members: [member.value for member in members],
        native_enum=False,
    )


def validate_run_relative_path(value: str, field_name: str) -> str:
    path = value.strip()
    if not path:
        raise ValueError(f"{field_name} must not be empty")
    if "\\" in path:
        raise ValueError(f"{field_name} must use POSIX separators")
    if ":" in path:
        raise ValueError(f"{field_name} must not contain drive or URI separators")
    if "//" in path:
        raise ValueError(f"{field_name} must not contain repeated separators")
    if path.startswith("~"):
        raise ValueError(f"{field_name} must be relative to the configured storage root")
    if path.startswith("./") or "/./" in path or path.endswith("/."):
        raise ValueError(f"{field_name} must not contain current-directory path parts")
    if path.endswith("/") or path.endswith("/.."):
        raise ValueError(f"{field_name} must not end with a directory traversal marker")
    parsed = PurePosixPath(path)
    if parsed.is_absolute():
        raise ValueError(f"{field_name} must be relative to the configured storage root")
    if any(part in {"", ".", ".."} for part in parsed.parts):
        raise ValueError(f"{field_name} must not contain empty, current, or parent path parts")
    return path


def resolve_run_artifact_path(storage_root: Path, artifact_root: str, artifact_path: str) -> Path:
    """Resolve a run artifact path and enforce containment under storage_root."""

    validated_artifact_root = validate_run_relative_path(artifact_root, "ProcessingRun.artifact_root")
    validated_artifact_path = validate_run_relative_path(artifact_path, "RunArtifact.path")
    resolved_storage_root = storage_root.expanduser().resolve()
    resolved = (resolved_storage_root / validated_artifact_root / validated_artifact_path).resolve()
    if resolved != resolved_storage_root and resolved_storage_root not in resolved.parents:
        raise ValueError("Resolved RunArtifact path escaped the configured storage root")
    return resolved


# ==========================================
# ENUMS (STRICT STATE MACHINES)
# ==========================================
class ReviewStatus(str, Enum):
    UNRESOLVED = "unresolved"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    QUARANTINED = "quarantined"


class JobKind(str, Enum):
    IMPORT = "import"
    PREPROCESS = "preprocess"
    SOURCE_HEALTH_SCAN = "source_health_scan"
    VAD = "vad"
    DIARIZATION = "diarization"
    SPEAKER_IDENTITY = "speaker_identity"
    TRUSTED_REGION_BUILD = "trusted_region_build"
    PROCESSING_BUFFER_BUILD = "processing_buffer_build"
    BUFFER_TRANSCRIPTION = "buffer_transcription"
    TRANSCRIPT_NORMALIZATION = "transcript_normalization"
    MFA_ALIGNMENT = "mfa_alignment"
    SAFE_CUTPOINT_SLICING = "safe_cutpoint_slicing"
    CANDIDATE_CLIP_ASSEMBLY = "candidate_clip_assembly"
    SPEAKER_PURITY_QC = "speaker_purity_qc"
    DATASET_QC = "dataset_qc"
    SLICE = "slice"
    SOURCE_TRANSCRIPTION = "source_transcription"
    SOURCE_ALIGNMENT = "source_alignment"
    SOURCE_SLICING = "source_slicing"
    INFERENCE = "inference"
    EXPORT = "export"


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ReferenceRunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ReferenceAssetStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    ARCHIVED = "archived"


class ReferenceEmbeddingStatus(str, Enum):
    MISSING = "missing"
    PENDING = "pending"
    READY = "ready"
    STALE = "stale"
    FAILED = "failed"


class ReferenceSourceKind(str, Enum):
    SOURCE_RECORDING = "source_recording"
    REFERENCE_VARIANT = "reference_variant"


class AudioVariantKind(str, Enum):
    ORIGINAL_SOURCE = "original_source_audio"
    NATIVE = "native_audio"
    ANALYSIS = "analysis_audio"
    TRAINING_EXPORT = "training_export_audio"


class PipelineArtifactStatus(str, Enum):
    PENDING = "pending"
    OK = "ok"
    NEEDS_REVIEW = "needs_review"
    REJECTED = "rejected"
    FAILED = "failed"


class ProcessingBufferStatus(str, Enum):
    PENDING = "pending"
    READY = "ready"
    COMPLETED = "completed"
    NEEDS_REVIEW = "needs_review"
    REJECTED = "rejected"
    FAILED = "failed"


class QCDecisionBucket(str, Enum):
    ACCEPTED = "accepted"
    NEEDS_REVIEW = "needs_review"
    REJECTED = "rejected"


class ProcessingRunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    NEEDS_REVIEW = "needs_review"
    REJECTED = "rejected"
    FAILED = "failed"


class RunArtifactStatus(str, Enum):
    MATERIALIZED = "materialized"
    REGENERATABLE = "regeneratable"
    DELETED = "deleted"
    FAILED = "failed"


class RfcStage(str, Enum):
    INGEST = "ingest"
    AUDIO_VARIANTS = "audio_variants"
    SOURCE_HEALTH = "source_health"
    VAD = "vad"
    DIARIZATION = "diarization"
    SPEAKER_IDENTITY = "speaker_identity"
    TRUSTED_REGIONS = "trusted_regions"
    PROCESSING_BUFFERS = "processing_buffers"
    ASR = "asr"
    NORMALIZATION = "normalization"
    MFA = "mfa"
    SAFE_CUTPOINTS = "safe_cutpoints"
    CANDIDATE_CLIPS = "candidate_clips"
    TRANSCRIPT_QC = "transcript_qc"
    SPEAKER_PURITY = "speaker_purity"
    DATASET_QC = "dataset_qc"
    EXPORT = "export"


class RunArtifactKind(str, Enum):
    RUN_CONFIG_JSON = "run_config_json"
    RUN_STATUS_JSON = "run_status_json"
    RUNTIME_VERSIONS_JSON = "runtime_versions_json"
    SOURCE_AUDIO_MANIFEST_JSON = "source_audio_manifest_json"
    SOURCE_AUDIO_SUMMARY_JSON = "source_audio_summary_json"
    AUDIO_VARIANTS_MANIFEST_JSON = "audio_variants_manifest_json"
    AUDIO_VARIANTS_SUMMARY_JSON = "audio_variants_summary_json"
    SOURCE_HEALTH_JSON = "source_health_json"
    PREFLIGHT_JSON = "preflight_json"
    WORKER_LOG = "worker_log"
    WORKER_PROCESS_LOG = "worker_process_log"
    VAD_SEGMENTS_JSONL = "vad_segments_jsonl"
    VAD_SUMMARY_JSON = "vad_summary_json"
    DIARIZATION_RTTM = "diarization_rttm"
    SPEAKER_REGIONS_JSONL = "speaker_regions_jsonl"
    SPEAKER_REGIONS_SUMMARY_JSON = "speaker_regions_summary_json"
    SPEAKER_SAMPLES_MANIFEST_JSON = "speaker_samples_manifest_json"
    SPEAKER_SELECTION_JSON = "speaker_selection_json"
    SPEAKER_CARDS_JSON = "speaker_cards_json"
    TRUSTED_REGIONS_JSON = "trusted_regions_json"
    PROCESSING_BUFFERS_JSON = "processing_buffers_json"
    PROCESSING_BUFFER_SUMMARY_JSON = "processing_buffer_summary_json"
    ASR_MFA_QUEUE_JSON = "asr_mfa_queue_json"
    ASR_MFA_QUEUE_SUMMARY_JSON = "asr_mfa_queue_summary_json"
    REJECTED_BUFFERS_JSON = "rejected_buffers_json"
    ASR_TRANSCRIPTS_JSON = "asr_transcripts_json"
    ASR_TRANSCRIPTS_SUMMARY_JSON = "asr_transcripts_summary_json"
    TRANSCRIPT_HAZARDS_JSON = "transcript_hazards_json"
    SYMBOL_HAZARD_SUMMARY_JSON = "symbol_hazard_summary_json"
    NORMALIZED_TRANSCRIPTS_JSON = "normalized_transcripts_json"
    NORMALIZATION_SUMMARY_JSON = "normalization_summary_json"
    MFA_CORPUS_MANIFEST_JSON = "mfa_corpus_manifest_json"
    MFA_SUMMARY_JSON = "mfa_summary_json"
    MFA_OOV_WORDS_JSON = "mfa_oov_words_json"
    MFA_OOV_SUMMARY_JSON = "mfa_oov_summary_json"
    MFA_TEXTGRID = "mfa_textgrid"
    ALIGNED_WORDS_JSONL = "aligned_words_jsonl"
    ALIGNED_WORDS_SUMMARY_JSON = "aligned_words_summary_json"
    ALIGNMENT_QC_JSON = "alignment_qc_json"
    ALIGNMENT_QC_BY_BUFFER_JSON = "alignment_qc_by_buffer_json"
    ALIGNMENT_QC_SUMMARY_JSON = "alignment_qc_summary_json"
    SAFE_CUTPOINTS_JSONL = "safe_cutpoints_jsonl"
    SAFE_CUTPOINT_CANDIDATES_JSONL = "safe_cutpoint_candidates_jsonl"
    REJECTED_CUTPOINT_CANDIDATES_JSONL = "rejected_cutpoint_candidates_jsonl"
    SAFE_CUTPOINT_SUMMARY_JSON = "safe_cutpoint_summary_json"
    SLICEABLE_CORES_JSON = "sliceable_cores_json"
    CANDIDATE_CLIP_MANIFEST_JSON = "candidate_clip_manifest_json"
    CANDIDATE_REVIEW_MANIFEST_JSON = "candidate_review_manifest_json"
    CANDIDATE_REVIEW_REJECTED_JSON = "candidate_review_rejected_json"
    CANDIDATE_REVIEW_SUMMARY_JSON = "candidate_review_summary_json"
    QUALITY_DROPPED_JSON = "quality_dropped_json"
    TRANSCRIPT_QC_JSON = "transcript_qc_json"
    TRANSCRIPT_QC_SUMMARY_JSON = "transcript_qc_summary_json"
    TARGET_VOICEPRINT_JSON = "target_voiceprint_json"
    SPEAKER_PURITY_JSON = "speaker_purity_json"
    SPEAKER_PURITY_SUMMARY_JSON = "speaker_purity_summary_json"
    DATASET_QC_JSON = "dataset_qc_json"
    DATASET_QC_SUMMARY_JSON = "dataset_qc_summary_json"
    CLIP_LAB_STATE_JSON = "clip_lab_state_json"
    VOXCPM_MANIFEST_JSONL = "voxcpm_manifest_jsonl"
    EXPORT_MANIFEST_JSON = "export_manifest_json"
    EXPORT_AUDIT_JSON = "export_audit_json"
    EXPORT_SUMMARY_JSON = "export_summary_json"


RFC_PIPELINE_VERSION = "pretraining_rfc_v1"

RFC_PRETRAINING_JOB_DAG: tuple[tuple[RfcStage, tuple[RfcStage, ...]], ...] = (
    (RfcStage.INGEST, ()),
    (RfcStage.AUDIO_VARIANTS, (RfcStage.INGEST,)),
    (RfcStage.SOURCE_HEALTH, (RfcStage.AUDIO_VARIANTS,)),
    (RfcStage.VAD, (RfcStage.SOURCE_HEALTH,)),
    (RfcStage.DIARIZATION, (RfcStage.VAD,)),
    (RfcStage.SPEAKER_IDENTITY, (RfcStage.DIARIZATION,)),
    (RfcStage.TRUSTED_REGIONS, (RfcStage.SPEAKER_IDENTITY,)),
    (RfcStage.PROCESSING_BUFFERS, (RfcStage.TRUSTED_REGIONS,)),
    (RfcStage.ASR, (RfcStage.PROCESSING_BUFFERS,)),
    (RfcStage.NORMALIZATION, (RfcStage.ASR,)),
    (RfcStage.MFA, (RfcStage.NORMALIZATION,)),
    (RfcStage.SAFE_CUTPOINTS, (RfcStage.MFA,)),
    (RfcStage.CANDIDATE_CLIPS, (RfcStage.SAFE_CUTPOINTS,)),
    (RfcStage.TRANSCRIPT_QC, (RfcStage.CANDIDATE_CLIPS,)),
    (RfcStage.SPEAKER_PURITY, (RfcStage.TRANSCRIPT_QC,)),
    (RfcStage.DATASET_QC, (RfcStage.SPEAKER_PURITY,)),
    (RfcStage.EXPORT, (RfcStage.DATASET_QC,)),
)


# ==========================================
# LEVEL 1: INGEST & RAW FILES
# ==========================================
class ImportBatch(SQLModel, table=True):
    """A logical grouping of uploaded files/folders."""

    id: str = Field(primary_key=True)
    name: str
    created_at: datetime = Field(default_factory=utc_now)
    active_prepared_output_group_id: str | None = None
    active_preparation_job_id: str | None = None

    recordings: list["SourceRecording"] = Relationship(back_populates="batch", cascade_delete=True)
    exports: list["ExportRun"] = Relationship(back_populates="batch", cascade_delete=True)


class SourceRecording(SQLModel, table=True):
    """
    Physical long-form .wav files.
    Self-referencing to track lineage (e.g., Raw -> UVR Music Removed -> Slicer)
    """

    id: str = Field(primary_key=True)
    batch_id: str = Field(foreign_key="importbatch.id")
    parent_recording_id: str | None = Field(default=None, foreign_key="sourcerecording.id")

    file_path: str
    sample_rate: int
    num_channels: int
    num_samples: int
    processing_recipe: str | None = None  # e.g., 'uvr_v5' if derived

    batch: ImportBatch = Relationship(back_populates="recordings")
    source_artifact: "SourceRecordingArtifact" = Relationship(
        back_populates="source_recording",
        sa_relationship_kwargs={"uselist": False},
        cascade_delete=True,
    )
    processing_jobs: list["ProcessingJob"] = Relationship(back_populates="source_recording", cascade_delete=True)

    @property
    def duration_s(self) -> float:
        return self.num_samples / self.sample_rate if self.sample_rate else 0.0


class SourceRecordingArtifact(SQLModel, table=True):
    """Recording-level transcript and alignment artifact metadata."""

    source_recording_id: str = Field(primary_key=True, foreign_key="sourcerecording.id")
    transcript_text_path: str | None = None
    transcript_json_path: str | None = None
    alignment_json_path: str | None = None
    transcript_status: str | None = None
    alignment_status: str | None = None
    transcript_word_count: int = 0
    alignment_word_count: int = 0
    transcript_updated_at: datetime | None = None
    aligned_at: datetime | None = None
    alignment_backend: str | None = None
    artifact_metadata: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))

    source_recording: SourceRecording | None = Relationship(back_populates="source_artifact")


# ==========================================
# RFC PRETRAINING ARTIFACT SPINE
# ==========================================
class ProcessingRun(SQLModel, table=True):
    """Coarse RFC run record. Heavy stage outputs live in RunArtifact files."""

    id: str = Field(primary_key=True)
    project_id: str = Field(foreign_key="importbatch.id", index=True)
    pipeline_version: str = RFC_PIPELINE_VERSION
    # Storage-root-relative run directory; RunArtifact.path is relative under this root.
    artifact_root: str | None = None
    stage: RfcStage = Field(sa_column=Column(sql_enum(RfcStage), index=True))
    status: ProcessingRunStatus = Field(default=ProcessingRunStatus.PENDING, sa_column=Column(sql_enum(ProcessingRunStatus), index=True))
    config_hash: str | None = None
    input_summary: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    output_summary: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    reason_codes: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=utc_now)
    started_at: datetime | None = None
    completed_at: datetime | None = None

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        if self.artifact_root is not None:
            self.artifact_root = self.validate_artifact_root(self.artifact_root)

    @staticmethod
    def validate_artifact_root(value: str) -> str:
        return validate_run_relative_path(value, "ProcessingRun.artifact_root")

    @validates("artifact_root")
    def validate_artifact_root_assignment(self, _key: str, value: str | None) -> str | None:
        return None if value is None else self.validate_artifact_root(value)


class SourceAudio(SQLModel, table=True):
    """RFC source-audio identity for immutable user input."""

    id: str = Field(primary_key=True)
    source_recording_id: str = Field(foreign_key="sourcerecording.id", index=True)
    pipeline_version: str = RFC_PIPELINE_VERSION
    source_hash: str | None = None
    file_path: str
    sample_rate: int
    num_channels: int
    num_samples: int
    duration_sec: float = 0.0
    created_at: datetime = Field(default_factory=utc_now)
    artifact_metadata: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))


class DatasetAudioVariant(SQLModel, table=True):
    """Source-level audio variant used by RFC stages."""

    __tablename__ = "datasetaudiovariant"

    id: str = Field(primary_key=True)
    source_audio_id: str = Field(foreign_key="sourceaudio.id", index=True)
    source_recording_id: str = Field(foreign_key="sourcerecording.id", index=True)
    kind: AudioVariantKind = Field(sa_column=Column(sql_enum(AudioVariantKind), index=True))
    file_path: str | None = None
    sample_rate: int | None = None
    num_channels: int | None = None
    num_samples: int | None = None
    source_start_sec: float = 0.0
    source_end_sec: float | None = None
    recipe: str | None = None
    content_hash: str | None = None
    materialization_status: str = "materialized"
    created_at: datetime = Field(default_factory=utc_now)
    artifact_metadata: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))


class SourceHealthScan(SQLModel, table=True):
    id: str = Field(primary_key=True)
    source_audio_id: str = Field(foreign_key="sourceaudio.id", index=True)
    source_recording_id: str = Field(foreign_key="sourcerecording.id", index=True)
    pipeline_version: str = RFC_PIPELINE_VERSION
    status: PipelineArtifactStatus = Field(sa_column=Column(sql_enum(PipelineArtifactStatus), index=True))
    duration_sec: float = 0.0
    sample_rate: int | None = None
    num_channels: int | None = None
    clipping_ratio: float | None = None
    rms: float | None = None
    silence_ratio: float | None = None
    reason_codes: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    metrics: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=utc_now)


class RunArtifact(SQLModel, table=True):
    """File-backed audit artifact index for high-cardinality RFC outputs."""

    id: str = Field(primary_key=True)
    run_id: str = Field(foreign_key="processingrun.id", index=True)
    project_id: str = Field(foreign_key="importbatch.id", index=True)
    source_audio_id: str | None = Field(default=None, foreign_key="sourceaudio.id", index=True)
    source_recording_id: str | None = Field(default=None, foreign_key="sourcerecording.id", index=True)
    kind: RunArtifactKind = Field(sa_column=Column(sql_enum(RunArtifactKind), index=True))
    path: str
    schema_version: int = 1
    byte_size: int | None = None
    content_hash: str | None = None
    config_hash: str | None = None
    input_artifact_hashes: dict[str, str] = Field(default_factory=dict, sa_column=Column(JSON))
    backend: str | None = None
    backend_version: str | None = None
    status: RunArtifactStatus = Field(default=RunArtifactStatus.MATERIALIZED, sa_column=Column(sql_enum(RunArtifactStatus), index=True))
    summary: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    reason_codes: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=utc_now)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        if self.path is not None:
            self.path = self.validate_relative_artifact_path(self.path)

    @staticmethod
    def validate_relative_artifact_path(value: str) -> str:
        return validate_run_relative_path(value, "RunArtifact.path")

    @validates("path")
    def validate_path_assignment(self, _key: str, value: str) -> str:
        return self.validate_relative_artifact_path(value)


class SpeakerIdentity(SQLModel, table=True):
    id: str = Field(primary_key=True)
    project_id: str = Field(foreign_key="importbatch.id", index=True)
    display_name: str | None = None
    linked_local_speakers: list[dict[str, str]] = Field(default_factory=list, sa_column=Column(JSON))
    embedding_profile_id: str | None = None
    created_by: str
    reason_codes: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=utc_now)


class CandidateClip(SQLModel, table=True):
    id: str = Field(primary_key=True)
    source_audio_id: str = Field(foreign_key="sourceaudio.id", index=True)
    source_recording_id: str = Field(foreign_key="sourcerecording.id", index=True)
    speaker_identity_id: str = Field(foreign_key="speakeridentity.id", index=True)
    processing_run_id: str = Field(foreign_key="processingrun.id", index=True)
    processing_buffer_artifact_id: str = Field(foreign_key="runartifact.id")
    processing_buffer_ref: str
    source_start_sec: float
    source_end_sec: float
    source_start_sample: int
    source_end_sample: int
    local_start_sec: float
    local_end_sec: float
    local_start_sample: int
    local_end_sample: int
    duration_sec: float
    cutpoint_artifact_id: str = Field(foreign_key="runartifact.id")
    start_cutpoint_ref: str
    end_cutpoint_ref: str
    aligned_words_artifact_id: str = Field(foreign_key="runartifact.id")
    word_ids: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    text: str
    audio_variant: str = "native"
    slicer_status: PipelineArtifactStatus = Field(default=PipelineArtifactStatus.PENDING, sa_column=Column(sql_enum(PipelineArtifactStatus), index=True))
    slicer_reason_codes: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=utc_now)


class TargetSpeakerProfile(SQLModel, table=True):
    id: str = Field(primary_key=True)
    speaker_identity_id: str = Field(foreign_key="speakeridentity.id", index=True)
    slicer_run_id: str | None = None
    reference_clip_ids: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    source_region_ids: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    embedding_backend: str
    embedding_backend_version: str | None = None
    centroid_embedding_path: str | None = None
    reference_duration_sec: float = 0.0
    reference_voiced_duration_sec: float = 0.0
    reference_speech_ratio: float = 0.0
    num_embedding_windows: int = 0
    num_windows_rejected_as_outliers: int = 0
    reference_quality: str = "weak"
    created_by: str
    reason_codes: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=utc_now)


class SpeakerPurityMetrics(SQLModel, table=True):
    id: str = Field(primary_key=True)
    candidate_clip_id: str = Field(foreign_key="candidateclip.id", index=True)
    target_profile_id: str = Field(foreign_key="targetspeakerprofile.id", index=True)
    target_full_similarity: float | None = None
    target_window_mean_similarity: float | None = None
    target_window_min_similarity: float | None = None
    target_window_p10_similarity: float | None = None
    foreign_window_count: int = 0
    foreign_window_duration_ms: int = 0
    embedding_std: float | None = None
    pairwise_window_sim_p10: float | None = None
    second_cluster_duration_ms: int = 0
    overlap_ms_inside_clip: int | None = None
    overlap_max_score: float | None = None
    pre_context_foreign_ms: int = 0
    post_context_foreign_ms: int = 0
    decision: QCDecisionBucket = Field(sa_column=Column(sql_enum(QCDecisionBucket), index=True))
    reason_codes: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    metrics: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=utc_now)


class ExportManifest(SQLModel, table=True):
    id: str = Field(primary_key=True)
    project_id: str = Field(foreign_key="importbatch.id", index=True)
    pipeline_version: str = RFC_PIPELINE_VERSION
    output_root: str
    manifest_path: str
    audit_manifest_path: str
    candidate_clip_ids: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    config_hash: str | None = None
    backend_versions: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    status: JobStatus = Field(default=JobStatus.PENDING, sa_column=Column(sql_enum(JobStatus), index=True))
    reason_codes: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=utc_now)
    completed_at: datetime | None = None


class ProcessingJob(SQLModel, table=True):
    """Background processing state shared across slicer and reference workflows."""

    id: str = Field(primary_key=True)
    kind: JobKind = Field(sa_column=Column(sql_enum(JobKind), index=True))
    status: JobStatus = Field(default=JobStatus.PENDING, sa_column=Column(sql_enum(JobStatus), index=True))
    source_recording_id: str | None = Field(default=None, foreign_key="sourcerecording.id")
    input_payload: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))
    output_payload: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))
    error_message: str | None = None
    claimed_by: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    started_at: datetime | None = None
    heartbeat_at: datetime | None = None
    completed_at: datetime | None = None

    source_recording: SourceRecording | None = Relationship(back_populates="processing_jobs")


class SourceRecordingView(SQLModel):
    id: str
    batch_id: str
    display_name: str | None = None
    parent_recording_id: str | None = None
    sample_rate: int
    num_channels: int
    num_samples: int
    processing_recipe: str | None = None
    duration_seconds: float = 0.0


class SourceRecordingArtifactView(SQLModel):
    source_recording_id: str
    transcript_text_path: str | None = None
    transcript_json_path: str | None = None
    alignment_json_path: str | None = None
    transcript_status: str | None = None
    alignment_status: str | None = None
    transcript_word_count: int = 0
    alignment_word_count: int = 0
    transcript_updated_at: datetime | None = None
    aligned_at: datetime | None = None
    alignment_backend: str | None = None
    artifact_metadata: dict[str, Any] | None = None


class SourceRecordingQueueView(SQLModel):
    id: str
    batch_id: str
    parent_recording_id: str | None = None
    sample_rate: int
    num_channels: int
    num_samples: int
    processing_recipe: str | None = None
    duration_seconds: float = 0.0
    slice_count: int = 0
    processing_state: str = "idle"
    processing_message: str | None = None
    active_job: "ProcessingJobView | None" = None
    artifact: SourceRecordingArtifactView | None = None


class ProcessingJobView(SQLModel):
    id: str
    kind: JobKind
    status: JobStatus
    source_recording_id: str | None = None
    input_payload: dict[str, Any] | None = None
    output_payload: dict[str, Any] | None = None
    error_message: str | None = None
    claimed_by: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    heartbeat_at: datetime | None = None
    completed_at: datetime | None = None


class DatasetRunCreateRequest(SQLModel):
    source_recording_ids: list[str] = Field(default_factory=list)
    config: dict[str, Any] = Field(default_factory=dict)
    single_speaker: bool = True
    target_speaker_label: str = "speaker_0"
    language: str = "auto"
    whisper_model_size: Literal["large-v3", "base"] = "large-v3"
    stop_after: Literal[
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
        "transcript_qc",
        "speaker_purity",
        "native_export",
    ] = "alignment_qc"


class DatasetRunArtifactView(SQLModel):
    id: str
    kind: RunArtifactKind
    path: str
    byte_size: int | None = None
    content_hash: str | None = None
    summary: dict[str, Any] = Field(default_factory=dict)
    reason_codes: list[str] = Field(default_factory=list)


class DatasetRunView(SQLModel):
    id: str
    project_id: str
    pipeline_version: str
    artifact_root: str | None = None
    stage: RfcStage
    status: ProcessingRunStatus
    config_hash: str | None = None
    input_summary: dict[str, Any] = Field(default_factory=dict)
    output_summary: dict[str, Any] = Field(default_factory=dict)
    reason_codes: list[str] = Field(default_factory=list)
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    artifacts: list[DatasetRunArtifactView] = Field(default_factory=list)


class DatasetRunLogView(SQLModel):
    run_id: str
    path: str
    text: str
    truncated: bool = False


class DatasetSpeakerSampleView(SQLModel):
    sample_id: str
    speaker_id: str
    source_audio_id: str
    audio_path: str
    start_sample: int
    end_sample: int
    duration_sec: float


class DatasetSpeakerSelectionView(SQLModel):
    mode: str
    selected: bool
    target_speaker_id: str | None = None
    source: str
    available_speaker_ids: list[str] = Field(default_factory=list)
    updated_at: str | None = None


class DatasetSpeakerSelectionUpdateRequest(SQLModel):
    target_speaker_id: str


class DatasetSpeakerResultsView(SQLModel):
    run_id: str
    speaker_regions_summary: dict[str, Any] = Field(default_factory=dict)
    speaker_samples_manifest: list[DatasetSpeakerSampleView] = Field(default_factory=list)
    speaker_selection: DatasetSpeakerSelectionView | None = None


class DatasetRunResumeRequest(SQLModel):
    stop_after: Literal["buffers", "normalization", "mfa", "alignment_qc"] = "alignment_qc"


class DatasetSlicerRerunRequest(SQLModel):
    config: dict[str, Any] = Field(default_factory=dict)


class DatasetSlicerResultsView(SQLModel):
    run_id: str
    safe_cutpoint_summary: dict[str, Any] = Field(default_factory=dict)
    candidate_review_summary: dict[str, Any] = Field(default_factory=dict)
    candidate_review_manifest: list[dict[str, Any]] = Field(default_factory=list)
    candidate_review_rejected: list[dict[str, Any]] = Field(default_factory=list)
    alignment_qc_by_buffer: list[dict[str, Any]] = Field(default_factory=list)
    transcripts: list[dict[str, Any]] = Field(default_factory=list)
    aligned_words: list[dict[str, Any]] = Field(default_factory=list)


class DatasetExportRerunRequest(SQLModel):
    config: dict[str, Any] = Field(default_factory=dict)


class DatasetExportResultsView(SQLModel):
    run_id: str
    export_summary: dict[str, Any] = Field(default_factory=dict)
    export_manifest: list[dict[str, Any]] = Field(default_factory=list)
    export_audit: list[dict[str, Any]] = Field(default_factory=list)


class DatasetQcDefaultsView(SQLModel):
    transcript_match_threshold: int = 85
    speaker_check_threshold: int = 70


class DatasetQcWeakSpanView(SQLModel):
    start_sec: float
    end_sec: float
    text: str | None = None
    score: float | None = None


class DatasetQcClipView(SQLModel):
    clip_id: str
    audio_path: str
    audio_url: str
    duration_sec: float
    training_text: str
    alignment_text: str | None = None
    # Null means the score is missing/unscored; frontend must treat null as rejected.
    transcript_match: float | None = None
    speaker_check: float | None = None
    transcript_reason_codes: list[str] = Field(default_factory=list)
    speaker_reason_codes: list[str] = Field(default_factory=list)
    candidate_reason_codes: list[str] = Field(default_factory=list)
    qc_reason_codes: list[str] = Field(default_factory=list)
    weak_transcript_spans: list[DatasetQcWeakSpanView] = Field(default_factory=list)
    weak_speaker_spans: list[DatasetQcWeakSpanView] = Field(default_factory=list)
    manual_override: Literal["force_keep", "force_reject"] | None = None


class DatasetQcFinalizedThresholdsView(SQLModel):
    transcript_match_min: int
    speaker_check_min: int


class DatasetQcPayloadView(SQLModel):
    run_id: str
    ready: bool
    missing_artifacts: list[str] = Field(default_factory=list)
    invalid_artifacts: list[str] = Field(default_factory=list)
    defaults: DatasetQcDefaultsView = Field(default_factory=DatasetQcDefaultsView)
    finalized: bool = False
    finalized_thresholds: DatasetQcFinalizedThresholdsView | None = None
    clips: list[DatasetQcClipView] = Field(default_factory=list)


class DatasetQcThresholdsRequest(SQLModel):
    transcript_match_min: int = Field(ge=0, le=100)
    speaker_check_min: int = Field(ge=0, le=100)

    @field_validator("transcript_match_min", "speaker_check_min", mode="before")
    @classmethod
    def strict_integer_threshold(cls, value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("threshold must be an integer 0-100")
        return value


class DatasetQcManualOverrideRequest(SQLModel):
    clip_id: str
    override: Literal["force_keep", "force_reject"]
    reason: str = ""


class DatasetQcFinalizeRequest(SQLModel):
    thresholds: DatasetQcThresholdsRequest
    manual_overrides: list[DatasetQcManualOverrideRequest] = Field(default_factory=list)


class DatasetQcGenerateRequest(SQLModel):
    force: bool = False


class DatasetQcFinalizeSummaryView(SQLModel):
    accepted_count: int
    rejected_count: int
    accepted_duration_sec: float
    rejected_duration_sec: float


class DatasetQcFinalizeResponse(SQLModel):
    run_id: str
    dataset_qc_path: str
    summary: DatasetQcFinalizeSummaryView


class DatasetClipLabPipelineFindingView(SQLModel):
    code: str
    label: str


class DatasetClipLabClipView(SQLModel):
    clip_id: str
    clip_version: int
    review_status: Literal["unresolved", "accepted", "rejected", "quarantined"]
    transcript: str
    original_transcript: str
    transcript_override: str | None = None
    reviewer_tags: list[str] = Field(default_factory=list)
    pipeline_findings: list[DatasetClipLabPipelineFindingView] = Field(default_factory=list)
    content_hash: str
    accepted_content_hash: str | None = None
    accepted_at: str | None = None
    acceptance_stale: bool = False
    transcript_match: float | None = None
    speaker_check: float | None = None
    sample_rate_hz: int | None = None
    effective_audio_kind: Literal["candidate_original", "rendered_revision"] | None = None
    effective_audio_revision_key: str | None = None
    source_audio_sha256: str | None = None
    audio_revision_hash: str | None = None
    rendered_audio_sha256: str | None = None
    audio_url: str | None = None
    waveform_peaks_url: str | None = None
    current_duration_sec: float | None = None
    audio_edit_op_count: int = 0
    audio_edit_ops: list[dict[str, Any]] = Field(default_factory=list)
    can_undo_audio: bool = False
    can_redo_audio: bool = False
    render_status: Literal["ready", "pending", "failed"] = "ready"


class DatasetClipLabAudioOperationRequest(SQLModel):
    expected_manifest_sha256: str
    expected_clip_version: int
    operation: dict[str, Any]


class DatasetClipLabAudioStackRequest(SQLModel):
    expected_manifest_sha256: str
    expected_clip_version: int


class DatasetClipLabView(SQLModel):
    run_id: str
    candidate_manifest_sha256: str
    stale_state: bool = False
    stale_reason: str | None = None
    invalid_state: bool = False
    invalid_state_reason: str | None = None
    saved_state_clip_count: int = 0
    qc_available: bool = False
    qc_error: str | None = None
    clips: list[DatasetClipLabClipView] = Field(default_factory=list)


class DatasetClipLabPatchRequest(SQLModel):
    expected_manifest_sha256: str
    expected_clip_version: int
    review_status: Literal["unresolved", "accepted", "rejected", "quarantined"] | None = None
    transcript_override: str | None = None
    reviewer_tags: list[str] | None = None


class ProjectPreparationRequest(SQLModel):
    target_sample_rate: int | None = None
    channel_mode: Literal["original", "mono", "left", "right"] = "original"


class ProjectPreparationRun(SQLModel):
    job: ProcessingJobView
    created_recordings: list[SourceRecordingView] = Field(default_factory=list)
    active_prepared_output_group_id: str | None = None


class SourceTranscriptionRequest(SQLModel):
    model_size: Literal["base", "small", "medium", "large-v3", "turbo"] = "turbo"
    batch_size: int = 8
    initial_prompt: str | None = None
    model_name: str | None = None
    model_version: str | None = None
    language_hint: str | None = None


class SourceAlignmentRequest(SQLModel):
    acoustic_model: str = "Wav2Vec2-Large-Robust-960h"
    text_normalization_strategy: Literal["strict", "loose", "spoken_form"] = "loose"
    batch_size: int = 8
    transcript_text_path: str | None = None
    transcript_json_path: str | None = None
    alignment_backend: str | None = None


class ProjectRecordingJobsRun(SQLModel):
    project_id: str
    prepared_output_group_id: str
    jobs: list[ProcessingJobView] = Field(default_factory=list)


class ReferencePickerRun(SQLModel, table=True):
    id: str = Field(primary_key=True)
    project_id: str = Field(foreign_key="importbatch.id")
    status: ReferenceRunStatus = Field(default=ReferenceRunStatus.QUEUED)
    mode: str = "both"
    config: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))
    artifact_root: str
    candidate_count: int = 0
    error_message: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    started_at: datetime | None = None
    completed_at: datetime | None = None


class ReferenceAsset(SQLModel, table=True):
    id: str = Field(primary_key=True)
    project_id: str = Field(foreign_key="importbatch.id")
    name: str
    status: ReferenceAssetStatus = Field(default=ReferenceAssetStatus.ACTIVE)
    transcript_text: str | None = None
    speaker_name: str | None = None
    language: str | None = None
    mood_label: str | None = None
    notes: str | None = None
    favorite_rank: int | None = None
    active_variant_id: str | None = None
    created_from_run_id: str | None = Field(default=None, foreign_key="referencepickerrun.id")
    created_from_candidate_id: str | None = None
    model_metadata: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class ReferenceVariant(SQLModel, table=True):
    id: str = Field(primary_key=True)
    reference_asset_id: str = Field(foreign_key="referenceasset.id")
    source_kind: ReferenceSourceKind
    source_recording_id: str | None = Field(default=None, foreign_key="sourcerecording.id")
    source_reference_variant_id: str | None = Field(default=None, foreign_key="referencevariant.id")
    source_start_seconds: float | None = None
    source_end_seconds: float | None = None
    file_path: str
    is_original: bool = False
    generator_model: str | None = None
    sample_rate: int
    num_samples: int
    model_metadata: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))
    deleted: bool = False
    created_at: datetime = Field(default_factory=utc_now)

    @property
    def duration_s(self) -> float:
        return self.num_samples / self.sample_rate if self.sample_rate else 0.0


class ExportRun(SQLModel, table=True):
    """Project-level export history for the current UI."""

    id: str = Field(primary_key=True)
    batch_id: str = Field(foreign_key="importbatch.id")
    status: JobStatus = Field(default=JobStatus.PENDING, sa_column=Column(sql_enum(JobStatus)))
    output_root: str
    manifest_path: str
    accepted_clip_count: int = 0
    failed_clip_count: int = 0
    created_at: datetime = Field(default_factory=utc_now)
    completed_at: datetime | None = None

    batch: ImportBatch | None = Relationship(back_populates="exports")


class ExportPreview(SQLModel):
    project_id: str
    manifest_path: str
    accepted_slice_count: int
    lines: list[str]


class ImportBatchCreate(SQLModel):
    id: str
    name: str


class ProjectSummary(SQLModel):
    id: str
    name: str
    created_at: datetime
    updated_at: datetime
    export_status: JobStatus | None = None
    active_prepared_output_group_id: str | None = None
    active_preparation_job_id: str | None = None


class SourceRecordingCreate(SQLModel):
    id: str
    batch_id: str
    file_path: str
    sample_rate: int
    num_channels: int
    num_samples: int
    parent_recording_id: str | None = None
    processing_recipe: str | None = None


class RecordingDerivativeCreate(SQLModel):
    id: str
    file_path: str
    sample_rate: int
    num_channels: int
    num_samples: int
    processing_recipe: str


class ReferenceVariantView(SQLModel):
    id: str
    reference_asset_id: str
    source_kind: ReferenceSourceKind
    source_recording_id: str | None = None
    source_reference_variant_id: str | None = None
    source_start_seconds: float | None = None
    source_end_seconds: float | None = None
    is_original: bool = False
    generator_model: str | None = None
    sample_rate: int
    num_samples: int
    deleted: bool = False
    created_at: datetime


class ReferenceAssetSummary(SQLModel):
    id: str
    project_id: str
    name: str
    status: ReferenceAssetStatus
    transcript_text: str | None = None
    speaker_name: str | None = None
    language: str | None = None
    mood_label: str | None = None
    active_variant_id: str | None = None
    created_from_run_id: str | None = None
    created_from_candidate_id: str | None = None
    source_slice_id: str | None = None
    source_audio_variant_id: str | None = None
    source_edit_commit_id: str | None = None
    embedding_status: ReferenceEmbeddingStatus = ReferenceEmbeddingStatus.MISSING
    embedding_space_id: str | None = None
    embedding_variant_id: str | None = None
    embedding_updated_at: datetime | None = None
    embedding_error_message: str | None = None
    created_at: datetime
    updated_at: datetime
    active_variant: ReferenceVariantView | None = None


class ReferenceAssetDetail(ReferenceAssetSummary):
    notes: str | None = None
    favorite_rank: int | None = None
    model_metadata: dict[str, Any] | None = None
    variants: list[ReferenceVariantView] = Field(default_factory=list)


class ReferenceRunCreate(SQLModel):
    recording_ids: list[str]
    mode: str = "both"
    target_durations: list[float] | None = None
    candidate_count_cap: int = 500
    dataset_run_id: str | None = None


class ReferenceRunView(SQLModel):
    id: str
    project_id: str
    status: ReferenceRunStatus
    mode: str
    config: dict[str, Any] | None = None
    candidate_count: int = 0
    embedding_space_id: str | None = None
    error_message: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None


class ReferenceCandidateSummary(SQLModel):
    candidate_id: str
    run_id: str
    source_media_kind: ReferenceSourceKind
    source_recording_id: str | None = None
    source_variant_id: str | None = None
    embedding_index: int | None = None
    embedding_space_id: str | None = None
    source_start_seconds: float
    source_end_seconds: float
    duration_seconds: float
    transcript_text: str | None = None
    speaker_name: str | None = None
    language: str | None = None
    risk_flags: list[str] = Field(default_factory=list)
    default_scores: dict[str, float] = Field(default_factory=dict)


class ReferenceAssetCreateFromCandidate(SQLModel):
    run_id: str
    candidate_id: str
    source_start_seconds: float | None = None
    source_end_seconds: float | None = None
    name: str | None = None
    mood_label: str | None = None


class MarkReferenceClipCandidateRequest(SQLModel):
    clip_id: str
    transcript_text: str


class ReferenceClipCandidateView(SQLModel):
    project_id: str
    dataset_run_id: str
    clip_id: str
    transcript_text: str
    filename: str
    relative_path: str
    source_audio_path: str
    created_at: str


class ReferenceRunRerankRequest(SQLModel):
    positive_candidate_ids: list[str] = Field(default_factory=list)
    negative_candidate_ids: list[str] = Field(default_factory=list)
    positive_reference_asset_ids: list[str] = Field(default_factory=list)
    negative_reference_asset_ids: list[str] = Field(default_factory=list)
    mode: str | None = None


class ReferenceCandidateRerankResult(ReferenceCandidateSummary):
    mode: str
    base_score: float
    intent_score: float
    rerank_score: float


class ReferenceRunRerankResponse(SQLModel):
    run_id: str
    mode: str
    embedding_space_id: str
    positive_candidate_ids: list[str] = Field(default_factory=list)
    negative_candidate_ids: list[str] = Field(default_factory=list)
    positive_reference_asset_ids: list[str] = Field(default_factory=list)
    negative_reference_asset_ids: list[str] = Field(default_factory=list)
    candidates: list[ReferenceCandidateRerankResult] = Field(default_factory=list)


class ReferenceEmbeddingEvaluationProbe(SQLModel):
    anchor_candidate_id: str
    expected_neighbor_candidate_ids: list[str] = Field(default_factory=list)
    top_k: int = 5


class ReferenceEmbeddingEvaluationRequest(SQLModel):
    probes: list[ReferenceEmbeddingEvaluationProbe] = Field(default_factory=list)


class ReferenceEmbeddingEvaluationProbeResult(SQLModel):
    anchor_candidate_id: str
    top_k: int
    retrieved_neighbor_candidate_ids: list[str] = Field(default_factory=list)
    matched_neighbor_candidate_ids: list[str] = Field(default_factory=list)
    recall_at_k: float = 0.0


class ReferenceEmbeddingEvaluationResponse(SQLModel):
    run_id: str
    embedding_space_id: str
    probe_count: int = 0
    average_recall_at_k: float = 0.0
    probes: list[ReferenceEmbeddingEvaluationProbeResult] = Field(default_factory=list)
