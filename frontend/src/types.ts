export type ReviewStatus = "unresolved" | "accepted" | "rejected" | "quarantined";

export type Tag = {
  id: string;
  name: string;
  color: string;
};

export type Transcript = {
  id: string;
  slice_id: string;
  original_text: string;
  modified_text?: string | null;
  is_modified: boolean;
  alignment_data?: Record<string, unknown> | null;
};

export type ClipRange = {
  start_seconds: number;
  end_seconds: number;
};

export type EditCommit = {
  id: string;
  slice_id: string;
  parent_commit_id?: string | null;
  edl_operations: {
    op: string;
    range?: ClipRange | null;
    duration_seconds?: number | null;
  }[];
  created_at: string;
};

export type AudioVariant = {
  id: string;
  slice_id: string;
  file_path: string;
  is_original: boolean;
  generator_model?: string | null;
  sample_rate: number;
  num_samples: number;
};

export type SourceRecording = {
  id: string;
  batch_id: string;
  parent_recording_id?: string | null;
  file_path: string;
  sample_rate: number;
  num_channels: number;
  num_samples: number;
  processing_recipe?: string | null;
};

export type Slice = {
  id: string;
  source_recording_id: string;
  active_variant_id?: string | null;
  active_commit_id?: string | null;
  status: ReviewStatus;
  duration_seconds: number;
  model_metadata?: Record<string, unknown> | null;
  created_at: string;
  source_recording: SourceRecording;
  transcript?: Transcript | null;
  tags: Tag[];
  variants: AudioVariant[];
  commits: EditCommit[];
  active_variant?: AudioVariant | null;
  active_commit?: EditCommit | null;
};

export type ImportBatch = {
  id: string;
  name: string;
  created_at: string;
};

export type Project = ImportBatch;

export type ExportRun = {
  id: string;
  batch_id: string;
  status: "pending" | "running" | "completed" | "failed";
  output_root: string;
  manifest_path: string;
  accepted_clip_count: number;
  failed_clip_count: number;
  created_at: string;
  completed_at?: string | null;
};

export type ExportPreview = {
  project_id: string;
  manifest_path: string;
  accepted_slice_count: number;
  lines: string[];
};

export type MediaCleanupResult = {
  project_id: string;
  deleted_slice_count: number;
  deleted_variant_count: number;
  deleted_file_count: number;
  skipped_reference_count: number;
  deleted_slice_ids: string[];
  deleted_variant_ids: string[];
};

export type WaveformPeaks = {
  clip_id: string;
  bins: number;
  peaks: number[];
};
