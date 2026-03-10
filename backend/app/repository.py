from __future__ import annotations

import io
import json
import math
import re
import wave
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

from .models import (
    Clip,
    ClipCommit,
    ClipCommitCreate,
    ClipEdlOperation,
    ClipEdlUpdate,
    ClipHistoryResult,
    ClipHistoryState,
    ClipSnapshot,
    ClipMutationResult,
    ClipRange,
    ClipSplitRequest,
    ClipStatusUpdate,
    ClipTag,
    ClipTagUpdate,
    EditState,
    ExportRun,
    ExportRunStatus,
    ExportStatus,
    ExportPreview,
    Project,
    ProjectDetail,
    ProjectImportResult,
    ProjectStats,
    RepositoryState,
    ReviewStatus,
    Transcript,
    WaveformPeaks,
    VoxCpmImportRequest,
    utc_now,
)


@dataclass
class FileBackedRepository:
    projects: dict[str, Project] = field(default_factory=dict)
    clips_by_project: dict[str, list[Clip]] = field(default_factory=dict)
    commits_by_clip: dict[str, list[ClipCommit]] = field(default_factory=dict)
    history_by_clip: dict[str, ClipHistoryState] = field(default_factory=dict)
    exports_by_project: dict[str, list[ExportRun]] = field(default_factory=dict)
    storage_path: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / "phase1-demo.json"
    )
    exports_root: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / "exports"
    )

    def __post_init__(self) -> None:
        if self.projects:
            return

        if self.storage_path.exists():
            self._load()
            self._normalize_all_project_order()
            self._save()
            return

        self._seed()
        self._normalize_all_project_order()
        self._save()

    def _seed(self) -> None:
        project = Project(
            id="phase1-demo",
            name="Phase 1 Demo Project",
            export_status=ExportStatus.NOT_EXPORTED,
        )

        clips = [
            Clip(
                id="clip-001",
                project_id=project.id,
                order_index=10,
                source_file_id="src-001",
                working_asset_id="asset-001",
                original_start_time=12.4,
                original_end_time=15.8,
                clip_edl=[
                    ClipEdlOperation(
                        op="delete_range",
                        range=ClipRange(start_seconds=0.0, end_seconds=0.12),
                    )
                ],
                review_status=ReviewStatus.CANDIDATE,
                edit_state=EditState.CLEAN,
                speaker_name="speaker_a",
                language="en",
                transcript=Transcript(
                    text_current="The workstation should make this painless.",
                    text_initial="The workstation should make this painless.",
                    source="whisper",
                    confidence=0.94,
                ),
                tags=[ClipTag(name="candidate", color="#8a7a3d")],
                duration_seconds=3.28,
                sample_rate=48000,
                channels=1,
            ),
            Clip(
                id="clip-002",
                project_id=project.id,
                order_index=20,
                source_file_id="src-001",
                working_asset_id="asset-001",
                original_start_time=16.1,
                original_end_time=19.6,
                clip_edl=[],
                review_status=ReviewStatus.NEEDS_ATTENTION,
                edit_state=EditState.DIRTY,
                speaker_name="speaker_a",
                language="en",
                transcript=Transcript(
                    text_current="We can recheck the end consonant here.",
                    text_initial="We can recheck the end consonant here.",
                    source="whisper",
                    confidence=0.71,
                ),
                tags=[
                    ClipTag(name="clipped_end", color="#c95f44"),
                    ClipTag(name="recheck", color="#2f6c8f"),
                ],
                duration_seconds=3.51,
                sample_rate=48000,
                channels=1,
            ),
            Clip(
                id="clip-003",
                project_id=project.id,
                order_index=30,
                source_file_id="src-002",
                working_asset_id="asset-004",
                original_start_time=2.0,
                original_end_time=4.4,
                clip_edl=[],
                review_status=ReviewStatus.ACCEPTED,
                edit_state=EditState.COMMITTED,
                speaker_name="speaker_a",
                language="en",
                transcript=Transcript(
                    text_current="This one is already ready for export.",
                    text_initial="This one is already ready for export.",
                    source="manual",
                    confidence=0.98,
                ),
                tags=[ClipTag(name="clean", color="#3c8452")],
                duration_seconds=2.4,
                sample_rate=48000,
                channels=1,
            ),
        ]

        self.projects[project.id] = project
        self.clips_by_project[project.id] = clips
        self.exports_by_project[project.id] = []
        self.commits_by_clip = {
            "clip-001": [],
            "clip-002": [],
            "clip-003": [
                ClipCommit(
                    id="commit-clip-003-initial",
                    clip_id="clip-003",
                    message="Initial accepted draft",
                    transcript_snapshot="This one is already ready for export.",
                    review_status_snapshot=ReviewStatus.ACCEPTED,
                    clip_edl_snapshot=[],
                    duration_seconds=2.4,
                    speaker_name="speaker_a",
                    language="en",
                )
            ],
        }
        for clip in clips:
            self.history_by_clip[clip.id] = ClipHistoryState(
                cursor=0,
                snapshots=[self._snapshot_from_clip(clip)],
            )

    def _load(self) -> None:
        state = RepositoryState.model_validate_json(self.storage_path.read_text())
        self.projects = state.projects
        self.clips_by_project = state.clips_by_project
        self.commits_by_clip = state.commits_by_clip
        self.history_by_clip = state.history_by_clip
        self.exports_by_project = state.exports_by_project
        self._ensure_runtime_state()

    def _save(self) -> None:
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        state = RepositoryState(
            projects=self.projects,
            clips_by_project=self.clips_by_project,
            commits_by_clip=self.commits_by_clip,
            history_by_clip=self.history_by_clip,
            exports_by_project=self.exports_by_project,
        )
        self.storage_path.write_text(state.model_dump_json(indent=2))

    def list_projects(self) -> list[Project]:
        return list(self.projects.values())

    def get_project(self, project_id: str) -> Project:
        return self.projects[project_id]

    def get_project_clips(self, project_id: str) -> list[Clip]:
        return self._get_active_project_clips(project_id)

    def get_project_detail(self, project_id: str) -> ProjectDetail:
        project = self.get_project(project_id)
        clips = self._get_active_project_clips(project_id)
        stats = self._calculate_stats(clips)
        return ProjectDetail(project=project, stats=stats, clips=clips)

    def list_export_runs(self, project_id: str) -> list[ExportRun]:
        self.get_project(project_id)
        return list(self.exports_by_project.get(project_id, []))

    def import_voxcpm_jsonl(self, payload: VoxCpmImportRequest) -> ProjectImportResult:
        jsonl_path = Path(payload.jsonl_path).expanduser()
        if not jsonl_path.exists():
            raise FileNotFoundError(f"JSONL file not found: {jsonl_path}")

        requested_name = (payload.project_name or "").strip()
        project_name = requested_name if requested_name else jsonl_path.stem
        requested_id = (payload.project_id or "").strip()
        base_id = requested_id if requested_id else self._slugify_project_id(project_name)
        project_id = self._unique_project_id(base_id)

        project = Project(
            id=project_id,
            name=project_name,
            export_status=ExportStatus.NOT_EXPORTED,
        )
        self.projects[project_id] = project
        self.clips_by_project[project_id] = []
        self.exports_by_project[project_id] = []

        imported_count = 0
        skipped_count = 0
        source_file_ids: dict[str, str] = {}
        working_asset_ids: dict[str, str] = {}

        with jsonl_path.open("r") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue

                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    skipped_count += 1
                    continue

                if not isinstance(entry, dict):
                    skipped_count += 1
                    continue

                audio_path = str(entry.get("audio", "")).strip()
                transcript_text = str(entry.get("text", "")).strip()
                if not audio_path or not transcript_text:
                    skipped_count += 1
                    continue

                parsed_duration = self._parse_duration(entry.get("duration"))
                sample_rate, channels, file_duration = self._read_wave_metadata(audio_path)
                duration_seconds = parsed_duration if parsed_duration is not None else file_duration
                if duration_seconds is None or duration_seconds <= 0:
                    skipped_count += 1
                    continue

                if audio_path not in source_file_ids:
                    source_file_ids[audio_path] = f"src-{len(source_file_ids) + 1:04d}"
                    working_asset_ids[audio_path] = f"asset-{len(working_asset_ids) + 1:04d}"

                clip = Clip(
                    id=f"clip-{uuid4().hex[:8]}",
                    project_id=project_id,
                    order_index=(imported_count + 1) * 10,
                    source_file_id=source_file_ids[audio_path],
                    working_asset_id=working_asset_ids[audio_path],
                    original_start_time=0.0,
                    original_end_time=round(duration_seconds, 2),
                    clip_edl=[],
                    review_status=ReviewStatus.CANDIDATE,
                    edit_state=EditState.CLEAN,
                    speaker_name=payload.speaker_name.strip() or "speaker_a",
                    language=payload.language.strip() or "en",
                    transcript=Transcript(
                        text_current=transcript_text,
                        text_initial=transcript_text,
                        source="import",
                        confidence=None,
                    ),
                    tags=[ClipTag(name="candidate", color="#8a7a3d")],
                    duration_seconds=round(duration_seconds, 2),
                    sample_rate=sample_rate,
                    channels=channels,
                )
                self.clips_by_project[project_id].append(clip)
                self.commits_by_clip[clip.id] = []
                self.history_by_clip[clip.id] = ClipHistoryState(
                    cursor=0,
                    snapshots=[self._snapshot_from_clip(clip)],
                )
                imported_count += 1

        if imported_count == 0:
            del self.projects[project_id]
            del self.clips_by_project[project_id]
            del self.exports_by_project[project_id]
            raise ValueError(
                "No valid clips were imported from the JSONL file. "
                "Each line must include audio, text, and a positive duration."
            )

        self._renumber_active_clips(project_id)
        self._save()

        return ProjectImportResult(
            project_detail=self.get_project_detail(project_id),
            imported_clip_count=imported_count,
            skipped_line_count=skipped_count,
        )

    def update_clip_status(self, clip_id: str, payload: ClipStatusUpdate) -> Clip:
        clip = self._find_clip(clip_id)
        clip.review_status = payload.review_status
        clip.edit_state = EditState.DIRTY
        clip.updated_at = utc_now()
        self._record_history(clip)
        self._touch_project(clip.project_id)
        self._save()
        return clip

    def update_clip_transcript(self, clip_id: str, text_current: str) -> Clip:
        clip = self._find_clip(clip_id)
        clip.transcript.text_current = text_current
        clip.transcript.updated_at = utc_now()
        clip.edit_state = EditState.DIRTY
        clip.updated_at = utc_now()
        self._record_history(clip)
        self._touch_project(clip.project_id)
        self._save()
        return clip

    def update_clip_tags(self, clip_id: str, payload: ClipTagUpdate) -> Clip:
        clip = self._find_clip(clip_id)
        clip.tags = payload.tags
        clip.edit_state = EditState.DIRTY
        clip.updated_at = utc_now()
        self._record_history(clip)
        self._touch_project(clip.project_id)
        self._save()
        return clip

    def append_edl_operation(self, clip_id: str, payload: ClipEdlUpdate) -> Clip:
        clip = self._find_clip(clip_id)
        operation = ClipEdlOperation(
            op=payload.op,
            range=payload.range,
            duration_seconds=payload.duration_seconds,
        )
        clip.clip_edl.append(operation)

        if payload.op == "delete_range" and payload.range is not None:
            removed = max(payload.range.end_seconds - payload.range.start_seconds, 0.0)
            clip.duration_seconds = max(round(clip.duration_seconds - removed, 2), 0.1)

        if payload.op == "insert_silence":
            clip.duration_seconds = round(
                clip.duration_seconds + max(payload.duration_seconds or 0.0, 0.0),
                2,
            )

        clip.edit_state = EditState.DIRTY
        clip.updated_at = utc_now()
        self._record_history(clip)
        self._touch_project(clip.project_id)
        self._save()
        return clip

    def get_clip_commits(self, clip_id: str) -> list[ClipCommit]:
        self._find_clip(clip_id)
        return self.commits_by_clip.get(clip_id, [])

    def commit_clip(self, clip_id: str, payload: ClipCommitCreate) -> ClipCommit:
        clip = self._find_clip(clip_id)
        commits = self.commits_by_clip.setdefault(clip.id, [])
        latest_commit = commits[-1] if commits else None

        commit = ClipCommit(
            id=f"commit-{uuid4().hex[:12]}",
            clip_id=clip.id,
            parent_commit_id=latest_commit.id if latest_commit else None,
            message=payload.message,
            transcript_snapshot=clip.transcript.text_current,
            review_status_snapshot=clip.review_status,
            clip_edl_snapshot=[entry.model_copy(deep=True) for entry in clip.clip_edl],
            duration_seconds=clip.duration_seconds,
            speaker_name=clip.speaker_name,
            language=clip.language,
        )

        commits.append(commit)
        clip.edit_state = EditState.COMMITTED
        clip.updated_at = utc_now()
        self._record_history(clip)
        self._touch_project(clip.project_id)
        self._save()
        return commit

    def undo_clip(self, clip_id: str) -> ClipHistoryResult:
        clip = self._find_clip(clip_id)
        history = self.history_by_clip.setdefault(
            clip.id,
            ClipHistoryState(cursor=0, snapshots=[self._snapshot_from_clip(clip)]),
        )
        if history.cursor <= 0:
            raise ValueError("No earlier edit state is available")

        history.cursor -= 1
        self._apply_snapshot_to_clip(clip, history.snapshots[history.cursor])
        clip.updated_at = utc_now()
        self._touch_project(clip.project_id)
        self._save()
        return self._build_history_result(clip, history)

    def redo_clip(self, clip_id: str) -> ClipHistoryResult:
        clip = self._find_clip(clip_id)
        history = self.history_by_clip.setdefault(
            clip.id,
            ClipHistoryState(cursor=0, snapshots=[self._snapshot_from_clip(clip)]),
        )
        if history.cursor >= len(history.snapshots) - 1:
            raise ValueError("No newer edit state is available")

        history.cursor += 1
        self._apply_snapshot_to_clip(clip, history.snapshots[history.cursor])
        clip.updated_at = utc_now()
        self._touch_project(clip.project_id)
        self._save()
        return self._build_history_result(clip, history)

    def split_clip(self, clip_id: str, payload: ClipSplitRequest) -> ClipMutationResult:
        clip = self._find_clip(clip_id)
        split_at = payload.split_at_seconds

        if split_at <= 0 or split_at >= clip.duration_seconds:
            raise ValueError("Split point must be inside the clip duration")

        split_ratio = split_at / clip.duration_seconds if clip.duration_seconds > 0 else 0.5
        left_text, right_text = self._split_transcript_text(
            clip.transcript.text_current,
            split_ratio=split_ratio,
        )

        left_clip = Clip(
            id=f"clip-{uuid4().hex[:8]}",
            project_id=clip.project_id,
            order_index=clip.order_index,
            source_file_id=clip.source_file_id,
            working_asset_id=clip.working_asset_id,
            audio_path=clip.audio_path,
            original_start_time=clip.original_start_time,
            original_end_time=round(clip.original_start_time + split_at, 2),
            clip_edl=[],
            review_status=ReviewStatus.IN_REVIEW,
            edit_state=EditState.DIRTY,
            speaker_name=clip.speaker_name,
            language=clip.language,
            transcript=Transcript(
                text_current=left_text,
                text_initial=left_text,
                source="manual",
                confidence=clip.transcript.confidence,
            ),
            tags=clip.tags,
            duration_seconds=round(split_at, 2),
            sample_rate=clip.sample_rate,
            channels=clip.channels,
        )

        right_duration = max(round(clip.duration_seconds - split_at, 2), 0.1)
        right_clip = Clip(
            id=f"clip-{uuid4().hex[:8]}",
            project_id=clip.project_id,
            order_index=clip.order_index + 1,
            source_file_id=clip.source_file_id,
            working_asset_id=clip.working_asset_id,
            audio_path=clip.audio_path,
            original_start_time=round(clip.original_start_time + split_at, 2),
            original_end_time=clip.original_end_time,
            clip_edl=[],
            review_status=ReviewStatus.IN_REVIEW,
            edit_state=EditState.DIRTY,
            speaker_name=clip.speaker_name,
            language=clip.language,
            transcript=Transcript(
                text_current=right_text,
                text_initial=right_text,
                source="manual",
                confidence=clip.transcript.confidence,
            ),
            tags=clip.tags,
            duration_seconds=right_duration,
            sample_rate=clip.sample_rate,
            channels=clip.channels,
        )

        clip.is_superseded = True
        clip.updated_at = utc_now()
        self._shift_order_indices_after(
            clip.project_id,
            clip.order_index,
            amount=2,
            exclude_ids={clip.id},
        )
        self.clips_by_project[clip.project_id].extend([left_clip, right_clip])
        self.commits_by_clip.setdefault(left_clip.id, [])
        self.commits_by_clip.setdefault(right_clip.id, [])
        self.history_by_clip[clip.id] = self.history_by_clip.get(
            clip.id,
            ClipHistoryState(cursor=0, snapshots=[self._snapshot_from_clip(clip)]),
        )
        self.history_by_clip[left_clip.id] = ClipHistoryState(
            cursor=0,
            snapshots=[self._snapshot_from_clip(left_clip)],
        )
        self.history_by_clip[right_clip.id] = ClipHistoryState(
            cursor=0,
            snapshots=[self._snapshot_from_clip(right_clip)],
        )
        self._renumber_active_clips(clip.project_id)
        self._touch_project(clip.project_id)
        self._save()

        return ClipMutationResult(
            operation="split",
            project_detail=self.get_project_detail(clip.project_id),
            created_clip_ids=[left_clip.id, right_clip.id],
            superseded_clip_ids=[clip.id],
        )

    def merge_with_next_clip(self, clip_id: str) -> ClipMutationResult:
        clip = self._find_clip(clip_id)
        active_clips = self._get_active_project_clips(clip.project_id)
        sorted_clips = self._sort_clips(active_clips)

        next_clip = None
        for index, candidate in enumerate(sorted_clips):
            if candidate.id == clip.id and index + 1 < len(sorted_clips):
                next_clip = sorted_clips[index + 1]
                break

        if next_clip is None:
            raise ValueError("No next active clip is available for merge")

        if (
            clip.source_file_id != next_clip.source_file_id
            or clip.working_asset_id != next_clip.working_asset_id
        ):
            raise ValueError("Merge is currently limited to clips from the same source asset")

        first_clip, second_clip = (
            (clip, next_clip)
            if clip.original_start_time <= next_clip.original_start_time
            else (next_clip, clip)
        )

        merged_tags = {tag.name: tag for tag in [*first_clip.tags, *second_clip.tags]}
        merged_clip = Clip(
            id=f"clip-{uuid4().hex[:8]}",
            project_id=clip.project_id,
            order_index=min(first_clip.order_index, second_clip.order_index),
            source_file_id=first_clip.source_file_id,
            working_asset_id=first_clip.working_asset_id,
            audio_path=first_clip.audio_path,
            original_start_time=first_clip.original_start_time,
            original_end_time=second_clip.original_end_time,
            clip_edl=[],
            review_status=ReviewStatus.IN_REVIEW,
            edit_state=EditState.DIRTY,
            speaker_name=first_clip.speaker_name,
            language=first_clip.language,
            transcript=Transcript(
                text_current=self._merge_transcript_text(
                    first_clip.transcript.text_current,
                    second_clip.transcript.text_current,
                ),
                text_initial=self._merge_transcript_text(
                    first_clip.transcript.text_current,
                    second_clip.transcript.text_current,
                ),
                source="manual",
                confidence=min(
                    first_clip.transcript.confidence or 1.0,
                    second_clip.transcript.confidence or 1.0,
                ),
            ),
            tags=list(merged_tags.values()),
            duration_seconds=round(
                first_clip.duration_seconds + second_clip.duration_seconds,
                2,
            ),
            sample_rate=first_clip.sample_rate,
            channels=first_clip.channels,
        )

        first_clip.is_superseded = True
        second_clip.is_superseded = True
        first_clip.updated_at = utc_now()
        second_clip.updated_at = utc_now()
        self.clips_by_project[clip.project_id].append(merged_clip)
        self.commits_by_clip.setdefault(merged_clip.id, [])
        self.history_by_clip.setdefault(
            first_clip.id,
            ClipHistoryState(cursor=0, snapshots=[self._snapshot_from_clip(first_clip)]),
        )
        self.history_by_clip.setdefault(
            second_clip.id,
            ClipHistoryState(cursor=0, snapshots=[self._snapshot_from_clip(second_clip)]),
        )
        self.history_by_clip[merged_clip.id] = ClipHistoryState(
            cursor=0,
            snapshots=[self._snapshot_from_clip(merged_clip)],
        )
        self._renumber_active_clips(clip.project_id)
        self._touch_project(clip.project_id)
        self._save()

        return ClipMutationResult(
            operation="merge",
            project_detail=self.get_project_detail(clip.project_id),
            created_clip_ids=[merged_clip.id],
            superseded_clip_ids=[first_clip.id, second_clip.id],
        )

    def get_export_preview(self, project_id: str) -> ExportPreview:
        project = self.get_project(project_id)
        accepted = self._get_export_eligible_clips(project_id)

        lines = [
            (
                f"exports/{project.id}/rendered/{clip.id}.wav|"
                f"{clip.speaker_name}|{clip.language}|{clip.transcript.text_current}"
            )
            for clip in accepted
        ]

        return ExportPreview(
            project_id=project.id,
            manifest_path=f"exports/{project.id}/dataset.list",
            accepted_clip_count=len(accepted),
            lines=lines,
        )

    def export_project(self, project_id: str) -> ExportRun:
        project = self.get_project(project_id)
        export_id = f"export-{uuid4().hex[:10]}"
        output_root = self.exports_root / project_id / export_id
        rendered_root = output_root / "rendered"
        manifest_path = output_root / "dataset.list"
        export_run = ExportRun(
            id=export_id,
            project_id=project_id,
            status=ExportRunStatus.RUNNING,
            output_root=str(output_root),
            manifest_path=str(manifest_path),
            accepted_clip_count=0,
        )
        self.exports_by_project.setdefault(project_id, []).append(export_run)
        project.export_status = ExportStatus.EXPORT_IN_PROGRESS
        project.updated_at = utc_now()
        self._save()

        committed = self._get_export_eligible_clips(project_id)

        try:
            rendered_root.mkdir(parents=True, exist_ok=True)
            manifest_lines: list[str] = []
            jsonl_lines: list[str] = []

            for clip in committed:
                rendered_path = rendered_root / f"{clip.id}.wav"
                rendered_path.write_bytes(self.get_clip_audio_bytes(clip.id))
                manifest_lines.append(
                    f"{rendered_path}|{clip.speaker_name}|{clip.language}|{clip.transcript.text_current}"
                )
                jsonl_lines.append(
                    json.dumps(
                        {
                            "audio": str(rendered_path),
                            "text": clip.transcript.text_current,
                            "duration": round(clip.duration_seconds, 2),
                        },
                        ensure_ascii=True,
                    )
                )

            manifest_path.write_text("\n".join(manifest_lines))
            dataset_jsonl_path = self._resolve_project_jsonl_output_path(project_id, committed)
            if dataset_jsonl_path is not None:
                dataset_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
                dataset_jsonl_path.write_text("\n".join(jsonl_lines))

            export_run.accepted_clip_count = len(committed)
            export_run.status = ExportRunStatus.SUCCEEDED
            export_run.completed_at = utc_now()
            project.export_status = ExportStatus.EXPORT_SUCCEEDED
            project.updated_at = utc_now()
            self._save()
            return export_run
        except Exception:
            export_run.status = ExportRunStatus.FAILED
            export_run.failed_clip_count = len(committed)
            export_run.completed_at = utc_now()
            project.export_status = ExportStatus.EXPORT_FAILED
            project.updated_at = utc_now()
            self._save()
            raise

    def get_waveform_peaks(self, clip_id: str, bins: int = 120) -> WaveformPeaks:
        clip = self._find_clip(clip_id)
        safe_bins = max(16, min(bins, 512))
        peaks = self._extract_waveform_peaks_from_file(clip, safe_bins)
        if peaks is None:
            peaks = [
                round(self._synthetic_peak_value(clip, index / safe_bins), 4)
                for index in range(safe_bins)
            ]
        return WaveformPeaks(clip_id=clip.id, bins=safe_bins, peaks=peaks)

    def get_clip_audio_bytes(self, clip_id: str) -> bytes:
        clip = self._find_clip(clip_id)
        audio_path = self._resolve_clip_audio_path(clip)
        if audio_path is not None and audio_path.exists():
            return audio_path.read_bytes()
        return self._render_clip_wave_bytes(clip)

    def _ensure_runtime_state(self) -> None:
        for project_id in self.projects:
            self.exports_by_project.setdefault(project_id, [])

        for clips in self.clips_by_project.values():
            for clip in clips:
                self.commits_by_clip.setdefault(clip.id, [])
                for commit in self.commits_by_clip[clip.id]:
                    if commit.duration_seconds <= 0:
                        commit.duration_seconds = clip.duration_seconds
                    if not commit.speaker_name.strip():
                        commit.speaker_name = clip.speaker_name
                    if not commit.language.strip():
                        commit.language = clip.language
                self.history_by_clip.setdefault(
                    clip.id,
                    ClipHistoryState(
                        cursor=0,
                        snapshots=[self._snapshot_from_clip(clip)],
                    ),
                )

    def _snapshot_from_clip(self, clip: Clip) -> ClipSnapshot:
        return ClipSnapshot(
            transcript_text=clip.transcript.text_current,
            review_status=clip.review_status,
            clip_edl=[entry.model_copy(deep=True) for entry in clip.clip_edl],
            tags=[tag.model_copy(deep=True) for tag in clip.tags],
            duration_seconds=clip.duration_seconds,
            edit_state=clip.edit_state,
        )

    def _apply_snapshot_to_clip(self, clip: Clip, snapshot: ClipSnapshot) -> None:
        clip.transcript.text_current = snapshot.transcript_text
        clip.transcript.updated_at = utc_now()
        clip.review_status = snapshot.review_status
        clip.clip_edl = [entry.model_copy(deep=True) for entry in snapshot.clip_edl]
        clip.tags = [tag.model_copy(deep=True) for tag in snapshot.tags]
        clip.duration_seconds = snapshot.duration_seconds
        clip.edit_state = snapshot.edit_state

    def _record_history(self, clip: Clip) -> None:
        history = self.history_by_clip.setdefault(
            clip.id,
            ClipHistoryState(cursor=0, snapshots=[self._snapshot_from_clip(clip)]),
        )
        snapshot = self._snapshot_from_clip(clip)

        if history.snapshots and history.snapshots[history.cursor].model_dump() == snapshot.model_dump():
            return

        if history.cursor < len(history.snapshots) - 1:
            history.snapshots = history.snapshots[: history.cursor + 1]

        history.snapshots.append(snapshot)
        if len(history.snapshots) > 50:
            history.snapshots = history.snapshots[-50:]
        history.cursor = len(history.snapshots) - 1

    def _build_history_result(
        self,
        clip: Clip,
        history: ClipHistoryState | None = None,
    ) -> ClipHistoryResult:
        current_history = history or self.history_by_clip.get(
            clip.id,
            ClipHistoryState(cursor=0, snapshots=[self._snapshot_from_clip(clip)]),
        )
        return ClipHistoryResult(
            clip=clip,
            can_undo=current_history.cursor > 0,
            can_redo=current_history.cursor < len(current_history.snapshots) - 1,
        )

    def _get_export_eligible_clips(self, project_id: str) -> list[Clip]:
        committed_clips: list[Clip] = []
        for clip in self._get_active_project_clips(project_id):
            latest_commit = self._get_latest_commit(clip.id)
            if latest_commit is None:
                continue

            committed_clip = self._build_export_clip_from_commit(clip, latest_commit)
            if (
                committed_clip.review_status == ReviewStatus.ACCEPTED
                and committed_clip.transcript.text_current.strip()
                and committed_clip.speaker_name.strip()
                and committed_clip.language.strip()
            ):
                committed_clips.append(committed_clip)
        return committed_clips

    def _get_latest_commit(self, clip_id: str) -> ClipCommit | None:
        commits = self.commits_by_clip.get(clip_id, [])
        return commits[-1] if commits else None

    def _build_export_clip_from_commit(self, clip: Clip, commit: ClipCommit) -> Clip:
        export_clip = clip.model_copy(deep=True)
        export_clip.review_status = commit.review_status_snapshot
        export_clip.edit_state = EditState.COMMITTED
        export_clip.transcript.text_current = commit.transcript_snapshot
        export_clip.transcript.updated_at = commit.created_at
        export_clip.clip_edl = [entry.model_copy(deep=True) for entry in commit.clip_edl_snapshot]
        export_clip.duration_seconds = commit.duration_seconds
        export_clip.speaker_name = commit.speaker_name
        export_clip.language = commit.language
        return export_clip

    def _resolve_project_jsonl_output_path(
        self,
        project_id: str,
        clips: list[Clip],
    ) -> Path | None:
        if not clips:
            return None

        directory_scores: dict[Path, int] = {}
        preferred_stems: dict[Path, str] = {}
        for clip in clips:
            if not clip.audio_path:
                continue
            audio_path = Path(clip.audio_path).expanduser()
            parent = audio_path.parent
            if not parent:
                continue

            # Prefer the dataset root one level above raw/segments folders when available.
            dataset_dir = parent.parent if parent.name.lower() in {"raw", "clips", "segments"} else parent
            directory_scores[dataset_dir] = directory_scores.get(dataset_dir, 0) + 1

            train_jsonl = dataset_dir / "train.jsonl"
            if train_jsonl.exists():
                preferred_stems[dataset_dir] = "train"
            elif dataset_dir not in preferred_stems:
                jsonl_candidates = sorted(dataset_dir.glob("*.jsonl"))
                if jsonl_candidates:
                    preferred_stems[dataset_dir] = jsonl_candidates[0].stem

        if not directory_scores:
            return self.exports_root / project_id / "committed-clips.jsonl"

        output_dir = max(directory_scores, key=lambda path: directory_scores[path])
        base_name = preferred_stems.get(output_dir, "train")
        return output_dir / f"{base_name}.committed.jsonl"

    def _synthetic_peak_value(self, clip: Clip, ratio: float) -> float:
        seed = sum(ord(char) for char in clip.id)
        base = 0.38 + 0.22 * math.sin((seed * 0.07) + (ratio * 10.4))
        harmonic = 0.28 * math.sin((seed * 0.03) + (ratio * 22.0))
        envelope = 0.82 - abs((ratio * 2) - 1) * 0.22
        return max(min(abs(base + harmonic) * envelope, 1.0), 0.04)

    def _render_clip_wave_bytes(self, clip: Clip) -> bytes:
        sample_rate = clip.sample_rate
        duration = max(clip.duration_seconds, 0.1)
        frame_count = max(int(sample_rate * duration), 1)
        seed = sum(ord(char) for char in clip.id)
        base_frequency = 180 + (seed % 120)
        harmonic_frequency = base_frequency * 2.1
        amplitude = 0.34
        fade_frames = max(int(sample_rate * 0.02), 1)
        buffer = io.BytesIO()

        with wave.open(buffer, "wb") as wave_file:
            wave_file.setnchannels(clip.channels)
            wave_file.setsampwidth(2)
            wave_file.setframerate(sample_rate)

            for frame_index in range(frame_count):
                t = frame_index / sample_rate
                envelope = 1.0
                if frame_index < fade_frames:
                    envelope *= frame_index / fade_frames
                if frame_count - frame_index <= fade_frames:
                    envelope *= (frame_count - frame_index) / fade_frames

                sample_value = (
                    amplitude * math.sin(2 * math.pi * base_frequency * t)
                    + 0.16 * math.sin(2 * math.pi * harmonic_frequency * t)
                ) * envelope
                pcm_value = max(min(int(sample_value * 32767), 32767), -32768)
                frame = pcm_value.to_bytes(2, byteorder="little", signed=True) * clip.channels
                wave_file.writeframesraw(frame)

        return buffer.getvalue()

    def _resolve_clip_audio_path(self, clip: Clip) -> Path | None:
        if not clip.audio_path:
            return None
        path = Path(clip.audio_path).expanduser()
        if not path.exists():
            return None
        return path

    def _extract_waveform_peaks_from_file(self, clip: Clip, bins: int) -> list[float] | None:
        audio_path = self._resolve_clip_audio_path(clip)
        if audio_path is None or audio_path.suffix.lower() != ".wav":
            return None

        try:
            with wave.open(str(audio_path), "rb") as wav_file:
                channels = wav_file.getnchannels()
                sample_width = wav_file.getsampwidth()
                frame_count = wav_file.getnframes()

                if frame_count <= 0 or channels <= 0 or sample_width != 2:
                    return None

                raw = wav_file.readframes(frame_count)
        except wave.Error:
            return None

        total_samples = frame_count
        samples_per_bin = max(total_samples // bins, 1)
        peaks: list[float] = []
        max_pcm = 32767.0

        for bin_index in range(bins):
            start_frame = bin_index * samples_per_bin
            end_frame = min((bin_index + 1) * samples_per_bin, total_samples)
            if start_frame >= total_samples:
                peaks.append(0.04)
                continue

            max_abs = 0
            for frame_index in range(start_frame, end_frame):
                frame_base = frame_index * channels * sample_width
                frame_peak = 0
                for channel_index in range(channels):
                    offset = frame_base + channel_index * sample_width
                    sample = int.from_bytes(
                        raw[offset : offset + sample_width],
                        byteorder="little",
                        signed=True,
                    )
                    frame_peak = max(frame_peak, abs(sample))
                max_abs = max(max_abs, frame_peak)

            normalized = max_abs / max_pcm if max_pcm > 0 else 0.0
            peaks.append(round(max(normalized, 0.04), 4))

        return peaks

    def _find_clip(self, clip_id: str) -> Clip:
        for clips in self.clips_by_project.values():
            for clip in clips:
                if clip.id == clip_id:
                    return clip
        raise KeyError(clip_id)

    def _get_active_project_clips(self, project_id: str) -> list[Clip]:
        return self._sort_clips(
            [clip for clip in self.clips_by_project[project_id] if not clip.is_superseded]
        )

    def _sort_clips(self, clips: list[Clip]) -> list[Clip]:
        return sorted(
            clips,
            key=lambda clip: (
                clip.order_index,
                clip.created_at,
            ),
        )

    def _normalize_all_project_order(self) -> None:
        for project_id in self.projects:
            self._renumber_active_clips(project_id)

    def _renumber_active_clips(self, project_id: str) -> None:
        active_clips = sorted(
            [clip for clip in self.clips_by_project[project_id] if not clip.is_superseded],
            key=lambda clip: (
                clip.order_index,
                clip.source_file_id,
                clip.original_start_time,
                clip.created_at,
            ),
        )
        for index, clip in enumerate(active_clips, start=1):
            clip.order_index = index * 10

    def _shift_order_indices_after(
        self,
        project_id: str,
        threshold: int,
        amount: int,
        exclude_ids: set[str] | None = None,
    ) -> None:
        exclude = exclude_ids or set()
        for candidate in self.clips_by_project[project_id]:
            if candidate.id in exclude or candidate.is_superseded:
                continue
            if candidate.order_index > threshold:
                candidate.order_index += amount

    def _touch_project(self, project_id: str) -> None:
        project = self.projects[project_id]
        project.export_status = ExportStatus.NOT_EXPORTED
        project.updated_at = utc_now()

    def _split_transcript_text(self, text: str, split_ratio: float = 0.5) -> tuple[str, str]:
        words = text.split()
        if len(words) < 2:
            return text, ""

        safe_ratio = max(0.1, min(split_ratio, 0.9))
        target_index = min(max(round(len(words) * safe_ratio), 1), len(words) - 1)

        candidate_indices = [
            index
            for index in range(1, len(words))
            if words[index - 1][-1:] in {".", ",", ";", ":", "?", "!"}
        ]

        if candidate_indices:
            split_index = min(
                candidate_indices,
                key=lambda candidate: abs(candidate - target_index),
            )
        else:
            split_index = target_index

        left = " ".join(words[:split_index]).strip()
        right = " ".join(words[split_index:]).strip()
        return left or text, right or text

    def _merge_transcript_text(self, first: str, second: str) -> str:
        parts = [part.strip() for part in [first, second] if part.strip()]
        if not parts:
            return ""
        if len(parts) == 1:
            return parts[0]

        first_part, second_part = parts
        joiner = " "
        if first_part[-1:] not in {".", ",", ";", ":", "?", "!"}:
            joiner = ". "
        return f"{first_part}{joiner}{second_part}"

    def _calculate_stats(self, clips: list[Clip]) -> ProjectStats:
        total_duration = sum(clip.duration_seconds for clip in clips)
        accepted = [clip for clip in clips if clip.review_status == ReviewStatus.ACCEPTED]
        rejected = [clip for clip in clips if clip.review_status == ReviewStatus.REJECTED]
        needs_attention = [
            clip for clip in clips if clip.review_status == ReviewStatus.NEEDS_ATTENTION
        ]

        return ProjectStats(
            total_clips=len(clips),
            accepted_clips=len(accepted),
            rejected_clips=len(rejected),
            needs_attention_clips=len(needs_attention),
            total_duration_seconds=round(total_duration, 2),
            accepted_duration_seconds=round(
                sum(clip.duration_seconds for clip in accepted), 2
            ),
        )

    def _slugify_project_id(self, value: str) -> str:
        cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
        return cleaned or "imported-project"

    def _unique_project_id(self, base_id: str) -> str:
        project_id = base_id
        suffix = 2
        while project_id in self.projects:
            project_id = f"{base_id}-{suffix}"
            suffix += 1
        return project_id

    def _parse_duration(self, value: object) -> float | None:
        if value is None:
            return None
        try:
            duration = float(value)
        except (TypeError, ValueError):
            return None
        return duration if duration > 0 else None

    def _read_wave_metadata(self, audio_path: str) -> tuple[int, int, float | None]:
        path = Path(audio_path).expanduser()
        if not path.exists() or path.suffix.lower() != ".wav":
            return 48000, 1, None

        try:
            with wave.open(str(path), "rb") as wave_file:
                sample_rate = wave_file.getframerate() or 48000
                channels = wave_file.getnchannels() or 1
                frames = wave_file.getnframes()
                duration = frames / sample_rate if sample_rate > 0 else None
                return sample_rate, channels, duration
        except wave.Error:
            return 48000, 1, None


repository = FileBackedRepository()
