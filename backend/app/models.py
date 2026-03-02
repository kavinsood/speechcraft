from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ReviewStatus(str, Enum):
    CANDIDATE = "candidate"
    IN_REVIEW = "in_review"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    NEEDS_ATTENTION = "needs_attention"


class EditState(str, Enum):
    CLEAN = "clean"
    DIRTY = "dirty"
    COMMITTED = "committed"


class ExportStatus(str, Enum):
    NOT_EXPORTED = "not_exported"
    EXPORT_IN_PROGRESS = "export_in_progress"
    EXPORT_SUCCEEDED = "export_succeeded"
    EXPORT_FAILED = "export_failed"


class ExportRunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ClipTag(BaseModel):
    name: str
    color: str


class Transcript(BaseModel):
    text_current: str
    text_initial: str
    source: str
    confidence: float | None = None
    updated_at: datetime = Field(default_factory=utc_now)


class ClipRange(BaseModel):
    start_seconds: float
    end_seconds: float


class ClipEdlOperation(BaseModel):
    op: str
    range: ClipRange | None = None
    duration_seconds: float | None = None


class Clip(BaseModel):
    id: str
    project_id: str
    order_index: int = 0
    source_file_id: str
    working_asset_id: str
    original_start_time: float
    original_end_time: float
    clip_edl: list[ClipEdlOperation] = Field(default_factory=list)
    review_status: ReviewStatus
    edit_state: EditState
    speaker_name: str
    language: str
    transcript: Transcript
    tags: list[ClipTag] = Field(default_factory=list)
    is_superseded: bool = False
    duration_seconds: float
    sample_rate: int
    channels: int
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class ProjectStats(BaseModel):
    total_clips: int
    accepted_clips: int
    rejected_clips: int
    needs_attention_clips: int
    total_duration_seconds: float
    accepted_duration_seconds: float


class Project(BaseModel):
    id: str
    name: str
    status: str = "active"
    export_status: ExportStatus = ExportStatus.NOT_EXPORTED
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class ProjectDetail(BaseModel):
    project: Project
    stats: ProjectStats
    clips: list[Clip]


class VoxCpmImportRequest(BaseModel):
    jsonl_path: str
    project_name: str | None = None
    project_id: str | None = None
    speaker_name: str = "speaker_a"
    language: str = "en"


class ProjectImportResult(BaseModel):
    project_detail: ProjectDetail
    imported_clip_count: int
    skipped_line_count: int


class ClipCommit(BaseModel):
    id: str
    clip_id: str
    parent_commit_id: str | None = None
    message: str
    transcript_snapshot: str
    review_status_snapshot: ReviewStatus
    clip_edl_snapshot: list[ClipEdlOperation] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utc_now)


class ExportPreview(BaseModel):
    project_id: str
    manifest_path: str
    accepted_clip_count: int
    lines: list[str]


class ClipStatusUpdate(BaseModel):
    review_status: ReviewStatus


class ClipTranscriptUpdate(BaseModel):
    text_current: str


class ClipCommitCreate(BaseModel):
    message: str = "Manual review commit"


class ClipEdlUpdate(BaseModel):
    op: str
    range: ClipRange | None = None
    duration_seconds: float | None = None


class ClipSplitRequest(BaseModel):
    split_at_seconds: float


class ClipMutationResult(BaseModel):
    operation: str
    project_detail: ProjectDetail
    created_clip_ids: list[str]
    superseded_clip_ids: list[str]


class ClipSnapshot(BaseModel):
    transcript_text: str
    review_status: ReviewStatus
    clip_edl: list[ClipEdlOperation] = Field(default_factory=list)
    tags: list[ClipTag] = Field(default_factory=list)
    duration_seconds: float
    edit_state: EditState


class ClipHistoryState(BaseModel):
    cursor: int = 0
    snapshots: list[ClipSnapshot] = Field(default_factory=list)


class ClipHistoryResult(BaseModel):
    clip: Clip
    can_undo: bool
    can_redo: bool


class ClipTagUpdate(BaseModel):
    tags: list[ClipTag]


class ExportRun(BaseModel):
    id: str
    project_id: str
    status: ExportRunStatus
    output_root: str
    manifest_path: str
    accepted_clip_count: int
    failed_clip_count: int = 0
    created_at: datetime = Field(default_factory=utc_now)
    completed_at: datetime | None = None


class WaveformPeaks(BaseModel):
    clip_id: str
    bins: int
    peaks: list[float]


class RepositoryState(BaseModel):
    projects: dict[str, Project]
    clips_by_project: dict[str, list[Clip]]
    commits_by_clip: dict[str, list[ClipCommit]]
    history_by_clip: dict[str, ClipHistoryState] = Field(default_factory=dict)
    exports_by_project: dict[str, list[ExportRun]] = Field(default_factory=dict)
