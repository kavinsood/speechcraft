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

export type TranscriptSummary = {
  id: string;
  slice_id: string;
  original_text: string;
  modified_text?: string | null;
  is_modified: boolean;
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
  transcript_text: string;
  status: ReviewStatus;
  tags: Tag[];
  active_variant_id?: string | null;
  message?: string | null;
  is_milestone: boolean;
  created_at: string;
};

export type AudioVariant = {
  id: string;
  slice_id: string;
  is_original: boolean;
  generator_model?: string | null;
  sample_rate: number;
  num_samples: number;
};

export type SourceRecording = {
  id: string;
  batch_id: string;
  display_name?: string | null;
  parent_recording_id?: string | null;
  sample_rate: number;
  num_channels: number;
  num_samples: number;
  processing_recipe?: string | null;
  duration_seconds: number;
};

export type DatasetRunStatus = "pending" | "running" | "completed" | "needs_review" | "rejected" | "failed";

export type DatasetRunArtifact = {
  id: string;
  kind: string;
  path: string;
  byte_size?: number | null;
  content_hash?: string | null;
  summary: Record<string, unknown>;
  reason_codes: string[];
};

export type DatasetRun = {
  id: string;
  project_id: string;
  pipeline_version: string;
  artifact_root?: string | null;
  stage: string;
  status: DatasetRunStatus;
  config_hash?: string | null;
  input_summary: Record<string, unknown>;
  output_summary: Record<string, unknown>;
  reason_codes: string[];
  created_at: string;
  started_at?: string | null;
  completed_at?: string | null;
  artifacts: DatasetRunArtifact[];
};

export type DatasetRunCreateRequest = {
  source_recording_ids: string[];
  config: Record<string, unknown>;
  single_speaker: boolean;
  target_speaker_label: string;
  stop_after: string;
};

export type DatasetSpeakerSample = {
  sample_id: string;
  speaker_id: string;
  source_audio_id: string;
  audio_path: string;
  start_sample: number;
  end_sample: number;
  duration_sec: number;
};

export type DatasetSpeakerSelection = {
  mode: string;
  selected: boolean;
  target_speaker_id?: string | null;
  source: string;
  available_speaker_ids: string[];
  updated_at?: string | null;
};

export type DatasetSpeakerResults = {
  run_id: string;
  speaker_regions_summary: Record<string, unknown>;
  speaker_samples_manifest: DatasetSpeakerSample[];
  speaker_selection?: DatasetSpeakerSelection | null;
};

export type DatasetRunLog = {
  run_id: string;
  path: string;
  text: string;
  truncated: boolean;
};

export type DatasetSlicerResults = {
  run_id: string;
  safe_cutpoint_summary: Record<string, unknown>;
  candidate_review_summary: Record<string, unknown>;
  candidate_review_manifest: Array<Record<string, unknown>>;
  candidate_review_rejected: Array<Record<string, unknown>>;
  alignment_qc_by_buffer?: Array<Record<string, unknown>>;
  transcripts?: Array<Record<string, unknown>>;
  aligned_words?: Array<Record<string, unknown>>;
};

export type DatasetExportResults = {
  run_id: string;
  export_summary: Record<string, unknown>;
  export_manifest: Array<Record<string, unknown>>;
  export_audit: Array<Record<string, unknown>>;
};

export type ManualOverride = "force_keep" | "force_reject";

export type DatasetQcWeakSpan = {
  start_sec: number;
  end_sec: number;
  text?: string | null;
  score?: number | null;
};

export type DatasetQcClip = {
  clip_id: string;
  audio_path: string;
  audio_url: string;
  duration_sec: number;
  training_text: string;
  alignment_text?: string | null;
  transcript_match: number | null;
  speaker_check: number | null;
  transcript_reason_codes: string[];
  speaker_reason_codes: string[];
  candidate_reason_codes: string[];
  qc_reason_codes: string[];
  weak_transcript_spans: DatasetQcWeakSpan[];
  weak_speaker_spans: DatasetQcWeakSpan[];
  manual_override?: ManualOverride | null;
};

export type DatasetQcPayload = {
  run_id: string;
  ready: boolean;
  missing_artifacts: string[];
  invalid_artifacts: string[];
  defaults: {
    transcript_match_threshold: number;
    speaker_check_threshold: number;
  };
  finalized: boolean;
  finalized_thresholds?: {
    transcript_match_min: number;
    speaker_check_min: number;
  } | null;
  clips: DatasetQcClip[];
};

export type DatasetQcFinalizeRequest = {
  thresholds: {
    transcript_match_min: number;
    speaker_check_min: number;
  };
  manual_overrides: Array<{
    clip_id: string;
    override: ManualOverride;
    reason?: string | null;
  }>;
};

export type DatasetQcFinalizeResponse = {
  run_id: string;
  dataset_qc_path: string;
  summary: {
    accepted_count: number;
    rejected_count: number;
    accepted_duration_sec: number;
    rejected_duration_sec: number;
  };
};

export type DatasetPreflight = {
  ok: boolean;
  reason_codes?: string[];
  error?: string;
  asr_model?: {
    ok: boolean;
    model?: string;
    error?: string | null;
    snapshot_check?: {
      ok: boolean;
      missing_files?: string[];
      error?: string | null;
    };
    [key: string]: unknown;
  };
  [key: string]: unknown;
};

export type SourceRecordingArtifact = {
  source_recording_id: string;
  transcript_text_path?: string | null;
  transcript_json_path?: string | null;
  alignment_json_path?: string | null;
  transcript_status?: string | null;
  alignment_status?: string | null;
  transcript_word_count: number;
  alignment_word_count: number;
  transcript_updated_at?: string | null;
  aligned_at?: string | null;
  alignment_backend?: string | null;
  artifact_metadata?: Record<string, unknown> | null;
};

export type ProcessingJob = {
  id: string;
  kind:
    | "import"
    | "preprocess"
    | "slice"
    | "source_transcription"
    | "source_alignment"
    | "source_slicing"
    | "review_window_asr"
    | "forced_align_and_pack"
    | "inference"
    | "export";
  status: "pending" | "running" | "completed" | "failed";
  source_recording_id?: string | null;
  input_payload?: Record<string, unknown> | null;
  output_payload?: Record<string, unknown> | null;
  error_message?: string | null;
  claimed_by?: string | null;
  created_at: string;
  started_at?: string | null;
  heartbeat_at?: string | null;
  completed_at?: string | null;
};

export type PreparationSettings = {
  target_sample_rate?: number | null;
  channel_mode: "original" | "mono" | "left" | "right";
};

export type ProjectTranscriptionSettings = {
  model_size: "base" | "small" | "medium" | "large-v3" | "turbo";
  batch_size: number;
  initial_prompt?: string | null;
};

export type ProjectAlignmentSettings = {
  acoustic_model: string;
  text_normalization_strategy: "strict" | "loose" | "spoken_form";
  batch_size: number;
};

export type ProjectPreparationRun = {
  job: ProcessingJob;
  created_recordings: SourceRecording[];
  active_prepared_output_group_id?: string | null;
};

export type ProjectRecordingJobsRun = {
  project_id: string;
  prepared_output_group_id: string;
  jobs: ProcessingJob[];
};

export type SourceRecordingQueue = SourceRecording & {
  duration_seconds: number;
  slice_count: number;
  processing_state: string;
  processing_message?: string | null;
  active_job?: ProcessingJob | null;
  artifact?: SourceRecordingArtifact | null;
};

export type SliceSummary = {
  id: string;
  source_recording_id: string;
  active_variant_id?: string | null;
  active_commit_id?: string | null;
  status: ReviewStatus;
  is_locked?: boolean;
  duration_seconds: number;
  model_metadata?: Record<string, unknown> | null;
  created_at: string;
  transcript?: TranscriptSummary | null;
  tags: Tag[];
  active_variant_generator_model?: string | null;
  can_undo: boolean;
  can_redo: boolean;
};

export type Slice = SliceSummary & {
  transcript?: Transcript | null;
  source_recording: SourceRecording;
  variants: AudioVariant[];
  commits: EditCommit[];
  active_variant?: AudioVariant | null;
  active_commit?: EditCommit | null;
};

export type ClipLabItemRef = {
  id: string;
};

export type ClipLabTranscript = {
  id: string;
  original_text: string;
  modified_text?: string | null;
  is_modified: boolean;
  alignment_data?: Record<string, unknown> | null;
};

export type ClipLabVariant = {
  id: string;
  is_original: boolean;
  generator_model?: string | null;
  sample_rate: number;
  num_samples: number;
};

export type ClipLabCommit = {
  id: string;
  parent_commit_id?: string | null;
  edl_operations: {
    op: string;
    range?: ClipRange | null;
    duration_seconds?: number | null;
  }[];
  transcript_text: string;
  status: ReviewStatus;
  tags: Tag[];
  active_variant_id?: string | null;
  message?: string | null;
  is_milestone: boolean;
  created_at: string;
};

export type ClipLabCapabilities = {
  can_edit_transcript: boolean;
  can_edit_tags: boolean;
  can_set_status: boolean;
  can_save: boolean;
  can_split: boolean;
  can_merge: boolean;
  can_edit_waveform: boolean;
  can_run_processing: boolean;
  can_switch_variants: boolean;
  can_export: boolean;
  can_finalize: boolean;
};

export type ClipLabItem = {
  id: string;
  kind: "slice";
  source_recording_id: string;
  source_recording: SourceRecording;
  start_seconds: number;
  end_seconds: number;
  duration_seconds: number;
  status?: ReviewStatus | null;
  is_locked?: boolean;
  created_at: string;
  transcript?: ClipLabTranscript | null;
  tags: Tag[];
  speaker_name?: string | null;
  language?: string | null;
  audio_url: string;
  item_metadata?: Record<string, unknown> | null;
  transcript_source?: string | null;
  can_run_asr: boolean;
  asr_placeholder_message?: string | null;
  active_variant_generator_model?: string | null;
  can_undo: boolean;
  can_redo: boolean;
  capabilities: ClipLabCapabilities;
  variants: ClipLabVariant[];
  commits: ClipLabCommit[];
  active_variant?: ClipLabVariant | null;
  active_commit?: ClipLabCommit | null;
};

export type ImportBatch = {
  id: string;
  name: string;
  created_at: string;
  updated_at: string;
  export_status?: "pending" | "running" | "completed" | "failed" | null;
  active_prepared_output_group_id?: string | null;
  active_preparation_job_id?: string | null;
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

export type ReferenceAssetStatus = "draft" | "active" | "archived";
export type ReferenceSourceKind = "source_recording" | "slice_variant" | "reference_variant";

export type ReferenceVariant = {
  id: string;
  reference_asset_id: string;
  source_kind: ReferenceSourceKind;
  source_recording_id?: string | null;
  source_slice_id?: string | null;
  source_audio_variant_id?: string | null;
  source_reference_variant_id?: string | null;
  source_start_seconds?: number | null;
  source_end_seconds?: number | null;
  is_original: boolean;
  generator_model?: string | null;
  sample_rate: number;
  num_samples: number;
  deleted: boolean;
  created_at: string;
};

export type ReferenceAssetSummary = {
  id: string;
  project_id: string;
  name: string;
  status: ReferenceAssetStatus;
  transcript_text?: string | null;
  speaker_name?: string | null;
  language?: string | null;
  mood_label?: string | null;
  active_variant_id?: string | null;
  created_from_run_id?: string | null;
  created_from_candidate_id?: string | null;
  source_slice_id?: string | null;
  source_audio_variant_id?: string | null;
  source_edit_commit_id?: string | null;
  created_at: string;
  updated_at: string;
  active_variant?: ReferenceVariant | null;
};

export type ReferenceAssetDetail = ReferenceAssetSummary & {
  notes?: string | null;
  favorite_rank?: number | null;
  model_metadata?: Record<string, unknown> | null;
  variants: ReferenceVariant[];
};

export type ReferenceRunStatus = "queued" | "running" | "completed" | "failed";

export type ReferenceRun = {
  id: string;
  project_id: string;
  status: ReferenceRunStatus;
  mode: "zero_shot" | "finetune" | "both" | string;
  config?: Record<string, unknown> | null;
  candidate_count: number;
  error_message?: string | null;
  created_at: string;
  started_at?: string | null;
  completed_at?: string | null;
};

export type ReferenceCandidate = {
  candidate_id: string;
  run_id: string;
  source_media_kind: ReferenceSourceKind;
  source_recording_id?: string | null;
  source_variant_id?: string | null;
  embedding_index?: number | null;
  source_start_seconds: number;
  source_end_seconds: number;
  duration_seconds: number;
  transcript_text?: string | null;
  speaker_name?: string | null;
  language?: string | null;
  risk_flags: string[];
  default_scores: Record<string, number>;
};

export type ReferenceRerankCandidate = ReferenceCandidate & {
  mode: string;
  base_score: number;
  intent_score: number;
  rerank_score: number;
};

export type ReferenceRunRerankResponse = {
  run_id: string;
  mode: string;
  positive_candidate_ids: string[];
  negative_candidate_ids: string[];
  candidates: ReferenceRerankCandidate[];
};
