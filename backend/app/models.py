from datetime import datetime, timezone
from enum import Enum
from typing import Any

from sqlmodel import Column, Field, JSON, Relationship, SQLModel


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


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
    SLICE = "slice"
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


class ReferenceSourceKind(str, Enum):
    SOURCE_RECORDING = "source_recording"
    SLICE_VARIANT = "slice_variant"
    REFERENCE_VARIANT = "reference_variant"


# ==========================================
# LINK TABLES (MANY-TO-MANY)
# ==========================================
class SliceTagLink(SQLModel, table=True):
    """Junction table for Many-to-Many Tags to Slices."""

    slice_id: str = Field(foreign_key="slice.id", primary_key=True)
    tag_id: str = Field(foreign_key="tag.id", primary_key=True)


# ==========================================
# LEVEL 1: INGEST & RAW FILES
# ==========================================
class ImportBatch(SQLModel, table=True):
    """A logical grouping of uploaded files/folders."""

    id: str = Field(primary_key=True)
    name: str
    created_at: datetime = Field(default_factory=utc_now)

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
    slices: list["Slice"] = Relationship(back_populates="source_recording", cascade_delete=True)

    @property
    def duration_s(self) -> float:
        return self.num_samples / self.sample_rate if self.sample_rate else 0.0


# ==========================================
# LEVEL 2: METADATA & TAGS
# ==========================================
class Tag(SQLModel, table=True):
    """Human-readable metadata (e.g., 'noisy', 'breathy'). NOT control flow."""

    id: str = Field(primary_key=True)
    name: str = Field(index=True, unique=True)
    color: str = "#FFFFFF"

    slices: list["Slice"] = Relationship(back_populates="tags", link_model=SliceTagLink)


class Transcript(SQLModel, table=True):
    """1-to-1 isolated text data to keep the Slice table fast."""

    id: str = Field(primary_key=True)
    slice_id: str = Field(foreign_key="slice.id", unique=True)

    original_text: str
    modified_text: str | None = None
    is_modified: bool = False

    # Store word-level or phoneme-level timings here without bloating the DB
    alignment_data: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))

    parent_slice: "Slice" = Relationship(back_populates="transcript")


# ==========================================
# LEVEL 3: PHYSICAL AUDIO VARIANTS (CLIP LAB)
# ==========================================
class AudioVariant(SQLModel, table=True):
    """Immutable 5-15s .wav files. V0=Slicer Output. V1+=Denoiser Output."""

    id: str = Field(primary_key=True)
    slice_id: str = Field(foreign_key="slice.id")

    file_path: str
    is_original: bool = False
    generator_model: str | None = None  # 'slicer', 'deepfilternet'

    sample_rate: int
    num_samples: int

    parent_slice: "Slice" = Relationship(
        back_populates="variants",
        sa_relationship_kwargs={"foreign_keys": "AudioVariant.slice_id"},
    )

    @property
    def duration_s(self) -> float:
        return self.num_samples / self.sample_rate if self.sample_rate else 0.0


# ==========================================
# LEVEL 4: EDIT DECISION LIST (WAVEFORM MATH)
# ==========================================
class EditCommit(SQLModel, table=True):
    """Immutable slice revision snapshots for undo/redo and milestones."""

    id: str = Field(primary_key=True)
    slice_id: str = Field(foreign_key="slice.id")
    parent_commit_id: str | None = Field(default=None, foreign_key="editcommit.id")

    edl_operations: list[dict[str, Any]] = Field(default_factory=list, sa_column=Column(JSON))
    transcript_text: str = ""
    status: ReviewStatus = Field(default=ReviewStatus.UNRESOLVED)
    tags_payload: list[dict[str, str]] = Field(default_factory=list, sa_column=Column(JSON))
    active_variant_id_snapshot: str | None = None
    message: str | None = None
    is_milestone: bool = False
    created_at: datetime = Field(default_factory=utc_now)


# ==========================================
# LEVEL 5: THE LOGICAL CONTAINER (THE SOURCE OF TRUTH)
# ==========================================
class Slice(SQLModel, table=True):
    """The central entity for the Labeling UI."""

    id: str = Field(primary_key=True)
    source_recording_id: str = Field(foreign_key="sourcerecording.id")

    # POINTER 1: Which physical file to play?
    active_variant_id: str | None = Field(default=None, foreign_key="audiovariant.id")
    # POINTER 2: Which math to apply to it?
    active_commit_id: str | None = Field(default=None, foreign_key="editcommit.id")

    status: ReviewStatus = Field(default=ReviewStatus.UNRESOLVED)

    # Escape hatch for weird model-specific config
    model_metadata: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=utc_now)

    # --- Relationships ---
    source_recording: SourceRecording = Relationship(back_populates="slices")
    transcript: Transcript | None = Relationship(back_populates="parent_slice", cascade_delete=True)
    tags: list[Tag] = Relationship(back_populates="slices", link_model=SliceTagLink)

    variants: list[AudioVariant] = Relationship(
        back_populates="parent_slice",
        sa_relationship_kwargs={"primaryjoin": "Slice.id==AudioVariant.slice_id"},
        cascade_delete=True,
    )

    commits: list[EditCommit] = Relationship(
        sa_relationship_kwargs={"primaryjoin": "Slice.id==EditCommit.slice_id"},
        cascade_delete=True,
    )

    active_variant: AudioVariant | None = Relationship(
        sa_relationship_kwargs={"primaryjoin": "Slice.active_variant_id==AudioVariant.id"},
    )

    active_commit: EditCommit | None = Relationship(
        sa_relationship_kwargs={"primaryjoin": "Slice.active_commit_id==EditCommit.id"},
    )


class TagView(SQLModel):
    id: str
    name: str
    color: str


class TranscriptView(SQLModel):
    id: str
    slice_id: str
    original_text: str
    modified_text: str | None = None
    is_modified: bool
    alignment_data: dict[str, Any] | None = None


class TranscriptSummaryView(SQLModel):
    id: str
    slice_id: str
    original_text: str
    modified_text: str | None = None
    is_modified: bool


class AudioVariantView(SQLModel):
    id: str
    slice_id: str
    is_original: bool = False
    generator_model: str | None = None
    sample_rate: int
    num_samples: int


class SourceRecordingView(SQLModel):
    id: str
    batch_id: str
    parent_recording_id: str | None = None
    sample_rate: int
    num_channels: int
    num_samples: int
    processing_recipe: str | None = None
    duration_seconds: float = 0.0


class SliceRevision(SQLModel):
    id: str
    slice_id: str
    parent_commit_id: str | None = None
    edl_operations: list[dict[str, Any]] = Field(default_factory=list)
    transcript_text: str = ""
    status: ReviewStatus
    tags: list["TagPayload"] = Field(default_factory=list)
    active_variant_id: str | None = None
    message: str | None = None
    is_milestone: bool = False
    created_at: datetime


class SliceSummary(SQLModel):
    id: str
    source_recording_id: str
    active_variant_id: str | None = None
    active_commit_id: str | None = None
    status: ReviewStatus
    duration_seconds: float = 0.0
    model_metadata: dict[str, Any] | None = None
    created_at: datetime
    transcript: TranscriptSummaryView | None = None
    tags: list[TagView] = Field(default_factory=list)
    active_variant_generator_model: str | None = None
    can_undo: bool = False
    can_redo: bool = False


class SliceDetail(SliceSummary):
    transcript: TranscriptView | None = None
    source_recording: SourceRecordingView
    variants: list[AudioVariantView] = Field(default_factory=list)
    commits: list[SliceRevision] = Field(default_factory=list)
    active_variant: AudioVariantView | None = None
    active_commit: SliceRevision | None = None


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
    source_slice_id: str | None = Field(default=None, foreign_key="slice.id")
    source_audio_variant_id: str | None = Field(default=None, foreign_key="audiovariant.id")
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
    status: JobStatus = Field(default=JobStatus.PENDING)
    output_root: str
    manifest_path: str
    accepted_clip_count: int = 0
    failed_clip_count: int = 0
    created_at: datetime = Field(default_factory=utc_now)
    completed_at: datetime | None = None

    batch: ImportBatch | None = Relationship(back_populates="exports")


class TagPayload(SQLModel):
    name: str
    color: str


class SliceStatusUpdate(SQLModel):
    status: ReviewStatus


class SliceTranscriptUpdate(SQLModel):
    modified_text: str


class SliceTagUpdate(SQLModel):
    tags: list[TagPayload]


class SliceSaveRequest(SQLModel):
    modified_text: str | None = None
    tags: list[TagPayload] | None = None
    status: ReviewStatus | None = None
    message: str | None = None
    is_milestone: bool = False


class ClipRange(SQLModel):
    start_seconds: float
    end_seconds: float


class SliceEdlUpdate(SQLModel):
    op: str
    range: ClipRange | None = None
    duration_seconds: float | None = None


class SliceSplitRequest(SQLModel):
    split_at_seconds: float


class ActiveVariantUpdate(SQLModel):
    active_variant_id: str


class AudioVariantRunRequest(SQLModel):
    generator_model: str


class ExportPreview(SQLModel):
    project_id: str
    manifest_path: str
    accepted_slice_count: int
    lines: list[str]


class MediaCleanupResult(SQLModel):
    project_id: str
    deleted_slice_count: int = 0
    deleted_variant_count: int = 0
    deleted_file_count: int = 0
    skipped_reference_count: int = 0
    deleted_slice_ids: list[str] = Field(default_factory=list)
    deleted_variant_ids: list[str] = Field(default_factory=list)


class ImportBatchCreate(SQLModel):
    id: str
    name: str


class ProjectSummary(SQLModel):
    id: str
    name: str
    created_at: datetime
    updated_at: datetime
    export_status: JobStatus | None = None


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


class SlicerChunkInput(SQLModel):
    id: str
    file_path: str
    sample_rate: int
    num_samples: int
    original_start_time: float
    original_end_time: float
    transcript_text: str
    transcript_source: str = "whisper"
    transcript_confidence: float | None = None
    speaker_name: str = "speaker_a"
    language: str = "en"
    order_index: int
    tags: list[TagPayload] = Field(default_factory=list)
    model_metadata: dict[str, Any] | None = None


class SlicerHandoffRequest(SQLModel):
    chunks: list[SlicerChunkInput]


class AudioVariantCreate(SQLModel):
    id: str | None = None
    file_path: str
    sample_rate: int
    num_samples: int
    generator_model: str


class ReferenceVariantView(SQLModel):
    id: str
    reference_asset_id: str
    source_kind: ReferenceSourceKind
    source_recording_id: str | None = None
    source_slice_id: str | None = None
    source_audio_variant_id: str | None = None
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
    created_at: datetime
    updated_at: datetime
    active_variant: ReferenceVariantView | None = None


class ReferenceAssetDetail(ReferenceAssetSummary):
    notes: str | None = None
    favorite_rank: int | None = None
    model_metadata: dict[str, Any] | None = None
    variants: list[ReferenceVariantView] = Field(default_factory=list)


class ReferenceAssetCreateFromSlice(SQLModel):
    slice_id: str
    name: str | None = None
    mood_label: str | None = None


class ReferenceRunCreate(SQLModel):
    recording_ids: list[str]
    mode: str = "both"
    target_durations: list[float] | None = None
    candidate_count_cap: int = 60


class ReferenceRunView(SQLModel):
    id: str
    project_id: str
    status: ReferenceRunStatus
    mode: str
    config: dict[str, Any] | None = None
    candidate_count: int = 0
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
    name: str | None = None
    mood_label: str | None = None


class ReferenceRunRerankRequest(SQLModel):
    positive_candidate_ids: list[str] = Field(default_factory=list)
    negative_candidate_ids: list[str] = Field(default_factory=list)
    mode: str | None = None


class ReferenceCandidateRerankResult(ReferenceCandidateSummary):
    mode: str
    base_score: float
    intent_score: float
    rerank_score: float


class ReferenceRunRerankResponse(SQLModel):
    run_id: str
    mode: str
    positive_candidate_ids: list[str] = Field(default_factory=list)
    negative_candidate_ids: list[str] = Field(default_factory=list)
    candidates: list[ReferenceCandidateRerankResult] = Field(default_factory=list)


class WaveformPeaks(SQLModel):
    clip_id: str
    bins: int
    peaks: list[float]
