export type ReviewStatus =
  | "candidate"
  | "in_review"
  | "accepted"
  | "rejected"
  | "needs_attention";

export type EditState = "clean" | "dirty" | "committed";

export type ClipTag = {
  name: string;
  color: string;
};

export type Transcript = {
  text_current: string;
  text_initial: string;
  source: string;
  confidence?: number | null;
  updated_at: string;
};

export type ClipRange = {
  start_seconds: number;
  end_seconds: number;
};

export type ClipEdlOperation = {
  op: string;
  range?: ClipRange | null;
  duration_seconds?: number | null;
};

export type Clip = {
  id: string;
  project_id: string;
  order_index: number;
  source_file_id: string;
  working_asset_id: string;
  audio_path?: string | null;
  original_start_time: number;
  original_end_time: number;
  clip_edl: ClipEdlOperation[];
  review_status: ReviewStatus;
  edit_state: EditState;
  speaker_name: string;
  language: string;
  transcript: Transcript;
  tags: ClipTag[];
  is_superseded: boolean;
  duration_seconds: number;
  sample_rate: number;
  channels: number;
  created_at: string;
  updated_at: string;
};

export type Project = {
  id: string;
  name: string;
  status: string;
  export_status: "not_exported" | "export_in_progress" | "export_succeeded" | "export_failed";
  created_at: string;
  updated_at: string;
};

export type ProjectStats = {
  total_clips: number;
  accepted_clips: number;
  rejected_clips: number;
  needs_attention_clips: number;
  total_duration_seconds: number;
  accepted_duration_seconds: number;
};

export type ProjectDetail = {
  project: Project;
  stats: ProjectStats;
  clips: Clip[];
};

export type ClipCommit = {
  id: string;
  clip_id: string;
  parent_commit_id?: string | null;
  message: string;
  transcript_snapshot: string;
  review_status_snapshot: ReviewStatus;
  clip_edl_snapshot: ClipEdlOperation[];
  created_at: string;
};

export type ExportPreview = {
  project_id: string;
  manifest_path: string;
  accepted_clip_count: number;
  lines: string[];
};

export type ClipMutationResult = {
  operation: string;
  project_detail: ProjectDetail;
  created_clip_ids: string[];
  superseded_clip_ids: string[];
};

export type ClipHistoryResult = {
  clip: Clip;
  can_undo: boolean;
  can_redo: boolean;
};

export type ExportRun = {
  id: string;
  project_id: string;
  status: "queued" | "running" | "succeeded" | "failed";
  output_root: string;
  manifest_path: string;
  accepted_clip_count: number;
  failed_clip_count: number;
  created_at: string;
  completed_at?: string | null;
};

export type WaveformPeaks = {
  clip_id: string;
  bins: number;
  peaks: number[];
};
