import { startTransition, useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import {
  ApiError,
  appendClipEdlOperation,
  appendDatasetAudioOperation,
  fetchClipLabItem,
  fetchDatasetClipLab,
  fetchDatasetQc,
  fetchDatasetSlicerResults,
  fetchProjectDatasetRuns,
  fetchProjectExports,
  fetchProjectRecordings,
  fetchProjectReferenceAssets,
  markDatasetClipAsReferenceCandidate,
  mergeWithNextClip,
  patchDatasetClipLabClip,
  redoClip,
  redoDatasetAudioOperation,
  resolveApiUrl,
  runClipLabModel,
  saveCurrentSliceAsReference,
  saveClipState,
  setActiveVariant,
  splitClip,
  undoClip,
  undoDatasetAudioOperation,
} from "../api";
import ErrorBoundary from "../ErrorBoundary";
import ClipQueuePane from "../workspace/ClipQueuePane";
import EditorPane from "../workspace/EditorPane";
import InspectorPane from "../workspace/InspectorPane";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";
import {
  buildDatasetTagReadOnlyConfig,
  isDatasetClipLabEditable,
  resolveDatasetClipTags,
  type DatasetClipLabLoadState,
} from "./labelPageDatasetHelpers";
import {
  buildReviewerTagSuggestions,
  createClipLabPatchCoordinator,
  type ClipPatchBuilder,
} from "../workspace/dataset-clip-lab-patch";
import { datasetAudioOperationFromEdlPayload, requireDatasetSampleRateHz } from "../workspace/audioSampleCoords";
import { usePipelineContext } from "../pipeline/PipelineContext";
import { isEligibleLabDatasetRun, resolveLabDatasetRunId } from "../pipeline/datasetRunHelpers";
import {
  buildDatasetQcScoreIndex,
  type ClipQueueSortMode,
  getSliceDuration,
  getSliceSpeakerPurityScore,
  getSliceTranscriptConfidence,
  sortClipsForQueue,
  type DatasetQcScores,
} from "../workspace/workspace-helpers";
import type {
  ClipLabItem,
  ClipLabItemRef,
  ClipLabCapabilities,
  ClipLabCommit,
  ClipLabTranscript,
  ClipLabVariant,
  DatasetClipLabClipRow,
  DatasetClipLabPatchRequest,
  DatasetClipLabView,
  DatasetAudioEditOperation,
  DatasetQcPayload,
  DatasetRun,
  DatasetSlicerResults,
  ExportRun,
  Project,
  ReferenceAssetSummary,
  ReviewStatus,
  Slice,
  SliceSummary,
  SourceRecordingQueue,
  Tag,
} from "../types";

type WorkspaceStatus = "loading" | "error" | "ready";

type LabelPageProps = {
  activeProject: Project | null;
  activeClipItem: ClipLabItemRef | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onActiveClipItemChange: (clipItem: ClipLabItemRef | null) => void;
  onRetryProjects: () => void;
  onHeaderActionsChange: (actions: ReactNode) => void;
};

type DatasetCandidateClip = Record<string, unknown>;

const RESERVED_REVIEW_STATUS_LABELS = new Set<ReviewStatus>([
  "accepted",
  "rejected",
  "quarantined",
  "unresolved",
]);

function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof ApiError) {
    return error.message;
  }

  if (error instanceof Error && error.message.trim()) {
    return error.message;
  }

  return fallback;
}

function summarizeSlice(slice: Slice): SliceSummary {
  return {
    id: slice.id,
    source_recording_id: slice.source_recording_id,
    active_variant_id: slice.active_variant_id,
    active_commit_id: slice.active_commit_id,
    status: slice.status,
    duration_seconds: slice.duration_seconds,
    model_metadata: slice.model_metadata,
    created_at: slice.created_at,
    transcript: slice.transcript,
    tags: slice.tags,
    active_variant_generator_model: slice.active_variant?.generator_model ?? null,
    can_undo: slice.can_undo,
    can_redo: slice.can_redo,
  };
}

function datasetVariant(row: DatasetCandidateClip): ClipLabVariant {
  return {
    id: `${String(row.id)}:variant:analysis`,
    is_original: true,
    generator_model: null,
    sample_rate: Number(row.sample_rate ?? 16000),
    num_samples: Number(row.duration_samples ?? 0),
  };
}

function datasetCapabilities(editable: boolean): ClipLabCapabilities {
  return {
    can_edit_transcript: editable,
    can_edit_tags: editable,
    can_set_status: editable,
    can_save: editable,
    can_split: false,
    can_merge: false,
    can_edit_waveform: editable,
    can_run_processing: false,
    can_switch_variants: false,
    can_export: false,
    can_finalize: false,
  };
}

function assertDatasetClipLabEditable(
  datasetMode: boolean,
  loadState: DatasetClipLabLoadState,
  view: DatasetClipLabView | null,
): void {
  if (!isDatasetClipLabEditable(datasetMode, loadState, view)) {
    throw new Error("Clip Lab state is unavailable, stale, or invalid. Reload before editing.");
  }
}

function datasetAudioOpsToEdl(
  ops: DatasetAudioEditOperation[],
  sampleRateHz: number,
): ClipLabCommit["edl_operations"] {
  if (!Number.isFinite(sampleRateHz) || sampleRateHz <= 0) {
    return [];
  }
  return ops.map((operation) => {
    if (operation.kind === "delete_range") {
      const startSeconds = (operation.start_sample ?? 0) / sampleRateHz;
      const endSeconds = (operation.end_sample ?? 0) / sampleRateHz;
      return {
        op: "delete_range",
        range: { start_seconds: startSeconds, end_seconds: endSeconds },
      };
    }
    const atSeconds = (operation.at_sample ?? 0) / sampleRateHz;
    const durationSeconds = (operation.duration_samples ?? 0) / sampleRateHz;
    return {
      op: "insert_silence",
      range: { start_seconds: atSeconds, end_seconds: atSeconds },
      duration_seconds: durationSeconds,
    };
  });
}

function datasetCommit(
  row: DatasetCandidateClip,
  transcriptText: string,
  status: ReviewStatus,
  tags: Tag[],
  message?: string | null,
): ClipLabCommit {
  return {
    id: `${String(row.id)}:commit:current`,
    parent_commit_id: null,
    edl_operations: [],
    transcript_text: transcriptText,
    status,
    tags,
    active_variant_id: `${String(row.id)}:variant:analysis`,
    message: message ?? "Dataset candidate clip",
    is_milestone: false,
    created_at: new Date(0).toISOString(),
  };
}

function datasetTranscript(row: DatasetCandidateClip, modifiedText: string | null): ClipLabTranscript {
  return {
    id: `${String(row.id)}:transcript`,
    original_text: String(row.training_text ?? ""),
    modified_text: modifiedText,
    is_modified: modifiedText !== null && modifiedText !== String(row.training_text ?? ""),
    alignment_data: {
      alignment_text: row.alignment_text ?? null,
      word_ids: Array.isArray(row.word_ids) ? row.word_ids : [],
      review_reason_codes: Array.isArray(row.review_reason_codes) ? row.review_reason_codes : [],
      buffer_warning_reason_codes: Array.isArray(row.buffer_warning_reason_codes) ? row.buffer_warning_reason_codes : [],
    },
  };
}

function datasetTranscriptSummary(row: DatasetCandidateClip, modifiedText: string | null) {
  return {
    id: `${String(row.id)}:transcript`,
    slice_id: String(row.id),
    original_text: String(row.training_text ?? ""),
    modified_text: modifiedText,
    is_modified: modifiedText !== null && modifiedText !== String(row.training_text ?? ""),
  };
}

function candidateReviewManifestIndex(clipId: string): number | null {
  const match = /^candidate_review_clip_(\d+)$/.exec(clipId);
  if (!match) {
    return null;
  }
  const index = Number(match[1]);
  return Number.isFinite(index) ? index : null;
}

function datasetSliceSummary(
  row: DatasetCandidateClip,
  sourceRecordingId: string,
  clipLabRow?: DatasetClipLabClipRow,
  qcScores?: DatasetQcScores | null,
  manifestIndex?: number,
): SliceSummary {
  const tags = resolveDatasetClipTags(row, clipLabRow);
  const modifiedText = clipLabRow?.transcript_override ?? null;
  const transcript = datasetTranscriptSummary(row, modifiedText);
  const sampleRate = Number(row.sample_rate ?? 16000);
  const sourceStartSample = Number(row.source_start_sample ?? 0);
  const sourceEndSample = Number(row.source_end_sample ?? 0);
  const originalStartTime =
    Number.isFinite(sampleRate) && sampleRate > 0 && Number.isFinite(sourceStartSample)
      ? sourceStartSample / sampleRate
      : 0;
  const originalEndTime =
    Number.isFinite(sampleRate) && sampleRate > 0 && Number.isFinite(sourceEndSample)
      ? sourceEndSample / sampleRate
      : originalStartTime + Number(row.duration_sec ?? 0);
  const orderIndex = manifestIndex ?? candidateReviewManifestIndex(String(row.id)) ?? 0;
  const transcriptMatch =
    clipLabRow?.transcript_match ??
    qcScores?.transcriptMatch ??
    (typeof row.transcript_match === "number"
      ? row.transcript_match
      : typeof row.transcript_match_score === "number"
        ? row.transcript_match_score
        : null);
  const speakerCheck =
    clipLabRow?.speaker_check ??
    qcScores?.speakerCheck ??
    (typeof row.speaker_check === "number"
      ? row.speaker_check
      : typeof row.speaker_check_score === "number"
        ? row.speaker_check_score
        : null);
  return {
    id: String(row.id),
    source_recording_id: sourceRecordingId,
    active_variant_id: `${String(row.id)}:variant:analysis`,
    active_commit_id: `${String(row.id)}:commit:current`,
    status: clipLabRow?.review_status ?? "unresolved",
    is_locked: false,
    duration_seconds: Number(row.duration_sec ?? 0),
    model_metadata: {
      candidate_review: true,
      source_audio_id: row.source_audio_id ?? null,
      start_cutpoint_ref: row.start_cutpoint_ref ?? null,
      end_cutpoint_ref: row.end_cutpoint_ref ?? null,
      needs_review: Boolean(row.needs_review),
      transcript_match: transcriptMatch,
      speaker_check: speakerCheck,
      original_start_time: originalStartTime,
      original_end_time: originalEndTime,
      source_start_sample: sourceStartSample,
      source_end_sample: sourceEndSample,
      sample_rate: sampleRate,
      order_index: orderIndex,
    },
    created_at: new Date(0).toISOString(),
    transcript: transcript,
    tags,
    active_variant_generator_model: null,
    can_undo: false,
    can_redo: false,
  };
}

function datasetClipLabItem(
  row: DatasetCandidateClip,
  sourceRecording: SourceRecordingQueue,
  runId: string,
  clipLabRow?: DatasetClipLabClipRow,
  qcScores?: DatasetQcScores | null,
  clipLabEditable = false,
): ClipLabItem {
  const tags = resolveDatasetClipTags(row, clipLabRow);
  const transcriptText = clipLabRow?.transcript ?? String(row.training_text ?? "");
  const status = clipLabRow?.review_status ?? "unresolved";
  const sampleRate = Number(clipLabRow?.sample_rate_hz ?? row.sample_rate ?? 16000);
  const durationSeconds = Number(
    clipLabRow?.current_duration_sec ?? row.duration_sec ?? 0,
  );
  const variant = {
    ...datasetVariant(row),
    sample_rate: sampleRate,
    num_samples: Math.max(0, Math.round(durationSeconds * sampleRate)),
  };
  const transcript = datasetTranscript(row, clipLabRow?.transcript_override ?? null);
  const audioEditOps = clipLabRow?.audio_edit_ops ?? [];
  const commit = {
    ...datasetCommit(row, transcriptText, status, tags, "Dataset candidate clip"),
    edl_operations: datasetAudioOpsToEdl(audioEditOps, sampleRate),
  };
  const transcriptMatch =
    clipLabRow?.transcript_match ??
    qcScores?.transcriptMatch ??
    (typeof row.transcript_match === "number"
      ? row.transcript_match
      : typeof row.transcript_match_score === "number"
        ? row.transcript_match_score
        : null);
  const speakerCheck =
    clipLabRow?.speaker_check ??
    qcScores?.speakerCheck ??
    (typeof row.speaker_check === "number"
      ? row.speaker_check
      : typeof row.speaker_check_score === "number"
        ? row.speaker_check_score
        : null);
  return {
    id: String(row.id),
    kind: "slice",
    source_recording_id: sourceRecording.id,
    source_recording: sourceRecording,
    start_seconds: Number(row.source_start_sample ?? 0) / Number(row.sample_rate ?? 16000),
    end_seconds: Number(row.source_end_sample ?? 0) / Number(row.sample_rate ?? 16000),
    duration_seconds: durationSeconds,
    status,
    is_locked: false,
    created_at: new Date(0).toISOString(),
    transcript,
    tags,
    speaker_name: null,
    language: "en",
    audio_url: clipLabRow?.audio_url ?? `/media/dataset-runs/${runId}/candidate-review/${String(row.id)}.wav`,
    item_metadata: {
      candidate_review: true,
      review_reason_codes: Array.isArray(row.review_reason_codes) ? row.review_reason_codes : [],
      buffer_warning_reason_codes: Array.isArray(row.buffer_warning_reason_codes) ? row.buffer_warning_reason_codes : [],
      source_start_sample: row.source_start_sample ?? null,
      source_end_sample: row.source_end_sample ?? null,
      transcript_match: transcriptMatch,
      speaker_check: speakerCheck,
      effective_audio_revision_key: clipLabRow?.effective_audio_revision_key ?? null,
      waveform_peaks_url: clipLabRow?.waveform_peaks_url ?? null,
      render_status: clipLabRow?.render_status ?? "ready",
      sample_rate_hz: sampleRate,
    },
    can_run_asr: false,
    active_variant_generator_model: null,
    can_undo: clipLabRow?.can_undo_audio ?? false,
    can_redo: clipLabRow?.can_redo_audio ?? false,
    capabilities: datasetCapabilities(clipLabEditable),
    variants: [variant],
    commits: [commit],
    active_variant: variant,
    active_commit: commit,
  };
}

export default function LabelPage({
  activeProject,
  activeClipItem,
  projectLoadStatus,
  projectLoadError,
  onActiveClipItemChange,
  onRetryProjects,
  onHeaderActionsChange,
}: LabelPageProps) {
  const { selectedLabDatasetRunId, selectedSlicerDatasetRunId, selectedQcDatasetRunId, selectLabDatasetRun } =
    usePipelineContext();
  const [workspaceStatus, setWorkspaceStatus] = useState<WorkspaceStatus>("loading");
  const [workspaceError, setWorkspaceError] = useState<string | null>(null);
  const [workspaceEmptyMessage, setWorkspaceEmptyMessage] = useState<string | null>(null);
  const [workspaceNotice, setWorkspaceNotice] = useState<string | null>(null);
  const [slices, setSlices] = useState<SliceSummary[]>([]);
  const [queueSortMode, setQueueSortMode] = useState<ClipQueueSortMode>("source_timeline");
  const [recordings, setRecordings] = useState<SourceRecordingQueue[]>([]);
  const [activeClip, setActiveClip] = useState<ClipLabItem | null>(null);
  const [visibleQueueClipIds, setVisibleQueueClipIds] = useState<string[]>([]);
  const [exportRuns, setExportRuns] = useState<ExportRun[]>([]);
  const [referenceAssets, setReferenceAssets] = useState<ReferenceAssetSummary[]>([]);
  const [datasetSlicerResults, setDatasetSlicerResults] = useState<DatasetSlicerResults | null>(null);
  const [datasetRuns, setDatasetRuns] = useState<DatasetRun[]>([]);
  const datasetRunId = useMemo(
    () =>
      resolveLabDatasetRunId(datasetRuns, [
        selectedLabDatasetRunId,
        selectedSlicerDatasetRunId,
        selectedQcDatasetRunId,
      ]),
    [datasetRuns, selectedLabDatasetRunId, selectedSlicerDatasetRunId, selectedQcDatasetRunId],
  );
  const datasetMode = Boolean(datasetRunId);
  const [datasetClipLab, setDatasetClipLabState] = useState<DatasetClipLabView | null>(null);
  const [datasetClipLabLoadState, setDatasetClipLabLoadStateInternal] =
    useState<DatasetClipLabLoadState>("idle");
  const datasetClipLabRef = useRef<DatasetClipLabView | null>(null);
  const datasetClipLabLoadStateRef = useRef<DatasetClipLabLoadState>("idle");
  const activeClipItemRef = useRef(activeClipItem);
  const rowUpdatedRef = useRef<(runId: string, updated: DatasetClipLabClipRow) => void>(() => {});
  const conflictRef = useRef<(runId: string, clipId: string) => Promise<void>>(async () => {});
  const [isSavingReference, setIsSavingReference] = useState(false);
  const latestWorkspaceRequestRef = useRef(0);
  const latestDetailRequestRef = useRef(0);

  const NO_DATASET_RUN_MESSAGE =
    "No dataset run with candidate clips yet. Complete Processing and Slicer, then return to Clip Lab.";

  function setDatasetClipLab(next: DatasetClipLabView | null) {
    datasetClipLabRef.current = next;
    setDatasetClipLabState(next);
  }

  function setDatasetClipLabLoadState(next: DatasetClipLabLoadState) {
    datasetClipLabLoadStateRef.current = next;
    setDatasetClipLabLoadStateInternal(next);
  }

  const patchCoordinator = useMemo(
    () =>
      createClipLabPatchCoordinator({
        getView: () => datasetClipLabRef.current,
        patchClip: patchDatasetClipLabClip,
        appendAudioOp: appendDatasetAudioOperation,
        undoAudio: undoDatasetAudioOperation,
        redoAudio: redoDatasetAudioOperation,
        onViewChange: (next) => {
          datasetClipLabRef.current = next;
          setDatasetClipLabState(next);
        },
        onRowUpdated: (runId, updated) => rowUpdatedRef.current(runId, updated),
        onConflict: (runId, clipId) => conflictRef.current(runId, clipId),
      }),
    [],
  );

  useEffect(() => {
    activeClipItemRef.current = activeClipItem;
  }, [activeClipItem]);

  const clipLabRowById = useMemo(
    () => new Map((datasetClipLab?.clips ?? []).map((row) => [row.clip_id, row])),
    [datasetClipLab],
  );

  function buildDatasetSlicesFromManifest(
    results: DatasetSlicerResults,
    recordingId: string,
    clipLabView: DatasetClipLabView | null,
    qcScoreIndex: Map<string, DatasetQcScores>,
  ): SliceSummary[] {
    const clipLabById = new Map((clipLabView?.clips ?? []).map((row) => [row.clip_id, row]));
    return results.candidate_review_manifest.map((row, index) =>
      datasetSliceSummary(
        row as DatasetCandidateClip,
        recordingId,
        clipLabById.get(String(row.id)),
        qcScoreIndex.get(String(row.id)) ?? null,
        index,
      ),
    );
  }

  rowUpdatedRef.current = (runId, updated) => {
    if (datasetClipLabRef.current?.run_id !== runId) {
      return;
    }
    refreshDatasetSliceForClip(updated.clip_id, updated);
    if (activeClipItemRef.current?.id === updated.clip_id) {
      refreshActiveDatasetClip(updated.clip_id, updated);
    }
  };

  conflictRef.current = async (runId, clipId) => {
    if (datasetRunId !== runId || datasetClipLabRef.current?.run_id !== runId) {
      return;
    }
    const reloaded = await fetchDatasetClipLab(runId);
    setDatasetClipLab(reloaded);
    const primaryRecording = recordings[0];
    if (primaryRecording && datasetSlicerResults) {
      const qcPayload = await fetchDatasetQc(runId).catch(() => null);
      const qcScoreIndex = buildDatasetQcScoreIndex(qcPayload);
      setSlices(
        buildDatasetSlicesFromManifest(
          datasetSlicerResults,
          primaryRecording.id,
          reloaded,
          qcScoreIndex,
        ),
      );
    }
    if (activeClipItemRef.current?.id === clipId) {
      const refreshedRow = reloaded.clips.find((clip) => clip.clip_id === clipId);
      if (refreshedRow) {
        refreshActiveDatasetClip(clipId, refreshedRow);
      }
    }
    setWorkspaceNotice("Clip Lab state was out of date and has been reloaded.");
  };

  function refreshDatasetSliceForClip(clipId: string, clipLabRow: DatasetClipLabClipRow) {
    const manifestRow = datasetSlicerResults?.candidate_review_manifest.find(
      (row) => String(row.id) === clipId,
    ) as DatasetCandidateClip | undefined;
    const primaryRecording = recordings[0];
    if (!manifestRow || !primaryRecording) {
      return;
    }
    const manifestIndex =
      datasetSlicerResults?.candidate_review_manifest.findIndex((row) => String(row.id) === clipId) ?? 0;
    const qcScores: DatasetQcScores | null =
      clipLabRow.transcript_match !== null || clipLabRow.speaker_check !== null
        ? {
            transcriptMatch: clipLabRow.transcript_match,
            speakerCheck: clipLabRow.speaker_check,
          }
        : null;
    const nextSummary = datasetSliceSummary(
      manifestRow,
      primaryRecording.id,
      clipLabRow,
      qcScores,
      manifestIndex >= 0 ? manifestIndex : undefined,
    );
    setSlices((current) => current.map((slice) => (slice.id === clipId ? nextSummary : slice)));
  }

  function refreshActiveDatasetClip(clipId: string, clipLabRow: DatasetClipLabClipRow) {
    const manifestRow = datasetSlicerResults?.candidate_review_manifest.find(
      (row) => String(row.id) === clipId,
    ) as DatasetCandidateClip | undefined;
    const primaryRecording = recordings[0];
    if (!manifestRow || !primaryRecording || !datasetRunId) {
      return;
    }
    const qcScores: DatasetQcScores | null =
      clipLabRow.transcript_match !== null || clipLabRow.speaker_check !== null
        ? {
            transcriptMatch: clipLabRow.transcript_match,
            speakerCheck: clipLabRow.speaker_check,
          }
        : null;
    setActiveClip(
      datasetClipLabItem(
        manifestRow,
        primaryRecording,
        datasetRunId,
        clipLabRow,
        qcScores,
        isDatasetClipLabEditable(
          datasetMode,
          datasetClipLabLoadStateRef.current,
          datasetClipLabRef.current,
        ),
      ),
    );
  }

  function patchDatasetClipLab(
    clipId: string,
    buildPatch: ClipPatchBuilder,
  ): Promise<DatasetClipLabClipRow> {
    if (!datasetRunId) {
      throw new Error("Select a dataset run before editing Clip Lab state.");
    }
    assertDatasetClipLabEditable(
      datasetMode,
      datasetClipLabLoadStateRef.current,
      datasetClipLabRef.current,
    );
    return patchCoordinator.patchDatasetClipLab(datasetRunId, clipId, buildPatch);
  }

  async function loadWorkspace(projectId: string | null | undefined, options?: { silent?: boolean }) {
    const requestId = latestWorkspaceRequestRef.current + 1;
    latestWorkspaceRequestRef.current = requestId;
    if (!options?.silent) {
      setWorkspaceStatus("loading");
      setWorkspaceError(null);
      setWorkspaceEmptyMessage(null);
      setWorkspaceNotice(null);
    }

    if (!projectId) {
      setSlices([]);
      setRecordings([]);
      setExportRuns([]);
      setReferenceAssets([]);
      setDatasetClipLab(null);
      setDatasetClipLabLoadState("idle");
      onActiveClipItemChange(null);
      setVisibleQueueClipIds([]);
      setWorkspaceStatus("ready");
      setWorkspaceEmptyMessage("Select a project to open the review workstation.");
      return;
    }

    try {
      if (!datasetRunId) {
        if (latestWorkspaceRequestRef.current !== requestId) {
          return;
        }
        setDatasetSlicerResults(null);
        setDatasetClipLab(null);
        setDatasetClipLabLoadState("idle");
        setSlices([]);
        setRecordings([]);
        setExportRuns([]);
        setReferenceAssets([]);
        onActiveClipItemChange(null);
        setVisibleQueueClipIds([]);
        setWorkspaceStatus("ready");
        setWorkspaceEmptyMessage(NO_DATASET_RUN_MESSAGE);
        return;
      }

      const [results, nextRecordings, nextExports] = await Promise.all([
        fetchDatasetSlicerResults(datasetRunId),
        fetchProjectRecordings(projectId),
        fetchProjectExports(projectId),
      ]);
      let nextReferenceAssets: ReferenceAssetSummary[] = [];
      let datasetQcPayload: DatasetQcPayload | null = null;
      try {
        nextReferenceAssets = await fetchProjectReferenceAssets(projectId);
      } catch (error) {
        if (latestWorkspaceRequestRef.current === requestId) {
          setWorkspaceNotice(
            getErrorMessage(error, "The reference library did not load, so duplicate-save protection is unavailable right now."),
          );
        }
      }
      try {
        datasetQcPayload = await fetchDatasetQc(datasetRunId);
      } catch {
        datasetQcPayload = null;
      }
      let nextClipLab: DatasetClipLabView | null = null;
      let nextClipLabLoadState: DatasetClipLabLoadState = "loading";
      let clipLabLoadError: unknown = null;

      if (latestWorkspaceRequestRef.current === requestId) {
        setDatasetClipLabLoadState("loading");
      }

      try {
        nextClipLab = await fetchDatasetClipLab(datasetRunId);
        nextClipLabLoadState = "ready";
      } catch (error) {
        nextClipLabLoadState = "unavailable";
        clipLabLoadError = error;
      }

      if (latestWorkspaceRequestRef.current !== requestId) {
        return;
      }

      setDatasetClipLab(nextClipLab);
      setDatasetClipLabLoadState(nextClipLabLoadState);
      if (nextClipLabLoadState === "unavailable") {
        setWorkspaceNotice(
          getErrorMessage(
            clipLabLoadError,
            "Clip Lab state unavailable. Tags and review edits are disabled until you reload.",
          ),
        );
      }
      const primaryRecording = nextRecordings[0] ?? null;
      const qcScoreIndex = buildDatasetQcScoreIndex(datasetQcPayload);
      const clipLabById = new Map((nextClipLab?.clips ?? []).map((row) => [row.clip_id, row]));
      const nextSlices: SliceSummary[] = primaryRecording
        ? results.candidate_review_manifest.map((row, index) =>
            datasetSliceSummary(
              row as DatasetCandidateClip,
              primaryRecording.id,
              clipLabById.get(String(row.id)),
              qcScoreIndex.get(String(row.id)) ?? null,
              index,
            ),
          )
        : [];
      setDatasetSlicerResults(results);
      setSlices(nextSlices);
      setRecordings(nextRecordings);
      if (!options?.silent) {
        setActiveClip(null);
      }
      setExportRuns(nextExports);
      setReferenceAssets(nextReferenceAssets);
      const sortedSlices = sortClipsForQueue(nextSlices, queueSortMode);
      const nextActiveClip =
        activeClipItem && sortedSlices.some((slice) => slice.id === activeClipItem.id)
          ? activeClipItem
          : sortedSlices[0]
            ? { id: sortedSlices[0].id }
            : null;
      if (
        nextActiveClip?.id !== activeClipItem?.id
        || (nextActiveClip === null && activeClipItem !== null)
      ) {
        onActiveClipItemChange(nextActiveClip);
      }
      setWorkspaceStatus("ready");
      setWorkspaceEmptyMessage(
        nextSlices.length > 0 ? null : "This dataset run does not contain any candidate review clips.",
      );
    } catch (error) {
      if (latestWorkspaceRequestRef.current !== requestId) {
        return;
      }

      if (options?.silent) {
        setWorkspaceNotice(getErrorMessage(error, "Background workspace refresh failed."));
        return;
      }

      setSlices([]);
      setRecordings([]);
      setActiveClip(null);
      setExportRuns([]);
      setReferenceAssets([]);
      onActiveClipItemChange(null);
      setVisibleQueueClipIds([]);
      setWorkspaceStatus("error");
      setWorkspaceError(
        getErrorMessage(error, "The label workspace failed to load. Check the backend and try again."),
      );
    }
  }

  useEffect(() => {
    if (datasetRunId !== selectedLabDatasetRunId) {
      selectLabDatasetRun(datasetRunId);
    }
  }, [datasetRunId, selectedLabDatasetRunId, selectLabDatasetRun]);

  useEffect(() => {
    if (!activeProject?.id) {
      setDatasetRuns([]);
      return;
    }

    let cancelled = false;
    void fetchProjectDatasetRuns(activeProject.id)
      .then((runs) => {
        if (cancelled) {
          return;
        }
        setDatasetRuns(runs.filter(isEligibleLabDatasetRun));
      })
      .catch(() => {
        if (!cancelled) {
          setDatasetRuns([]);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [activeProject?.id]);

  function handleLabDatasetRunChange(runId: string) {
    patchCoordinator.resetQueues();
    selectLabDatasetRun(runId);
    setDatasetClipLab(null);
    setDatasetClipLabLoadState("idle");
    onActiveClipItemChange(null);
    setActiveClip(null);
  }

  useEffect(() => {
    if (projectLoadStatus === "error") {
      setWorkspaceStatus("error");
      setWorkspaceError(projectLoadError ?? "The project list failed to load.");
      return;
    }

    if (projectLoadStatus === "loading") {
      setWorkspaceStatus("loading");
      return;
    }

    void loadWorkspace(activeProject?.id);
  }, [activeProject?.id, projectLoadStatus, projectLoadError, datasetRunId]);

  const allClipTagNames = useMemo(() => {
    if (datasetMode && datasetClipLab) {
      return buildReviewerTagSuggestions(
        datasetClipLab.clips
          .flatMap((clip) => clip.reviewer_tags)
          .filter((tag) => !RESERVED_REVIEW_STATUS_LABELS.has(tag.toLocaleLowerCase() as ReviewStatus)),
      );
    }

    return Array.from(
      new Set(
        slices
          .flatMap((slice) => slice.tags)
          .map((tag) => tag.name.toLowerCase())
          .sort(),
      ),
    );
  }, [datasetClipLab, datasetMode, slices]);

  const sliceMap = useMemo(() => new Map(slices.map((slice) => [slice.id, slice])), [slices]);
  const queueSlices = useMemo(() => slices, [slices]);

  const activeSliceSummary = useMemo(() => {
    const visibleQueueClips = visibleQueueClipIds
      .map((clipId) => sliceMap.get(clipId) ?? null)
      .filter((slice): slice is SliceSummary => slice !== null);

    if (activeClipItem) {
      return visibleQueueClips.find((slice) => slice.id === activeClipItem.id) ?? visibleQueueClips[0] ?? slices[0] ?? null;
    }

    return visibleQueueClips[0] ?? slices[0] ?? null;
  }, [sliceMap, slices, activeClipItem, visibleQueueClipIds]);

  useEffect(() => {
    const requestId = latestDetailRequestRef.current + 1;
    latestDetailRequestRef.current = requestId;

    const nextActiveTarget = activeSliceSummary ? { id: activeSliceSummary.id } : null;

    if (!nextActiveTarget) {
      setActiveClip(null);
      return;
    }

    if (nextActiveTarget.id !== activeClipItem?.id) {
      onActiveClipItemChange(nextActiveTarget);
    }

    if (!datasetRunId) {
      setActiveClip(null);
      return;
    }

    const primaryRecording = recordings[0] ?? null;
    const candidateRow = datasetSlicerResults?.candidate_review_manifest.find(
      (row) => String(row.id) === nextActiveTarget.id,
    ) as DatasetCandidateClip | undefined;
    if (!primaryRecording || !candidateRow) {
      setActiveClip(null);
      setWorkspaceNotice("Dataset candidate clip could not be loaded.");
      return;
    }
    const summary = sliceMap.get(nextActiveTarget.id);
    const clipLabRow = clipLabRowById.get(nextActiveTarget.id);
    const qcScores: DatasetQcScores | null = summary
      ? {
          transcriptMatch: getSliceTranscriptConfidence(summary),
          speakerCheck: getSliceSpeakerPurityScore(summary),
        }
      : null;
    setActiveClip(
      datasetClipLabItem(
        candidateRow,
        primaryRecording,
        datasetRunId,
        clipLabRow,
        qcScores,
        isDatasetClipLabEditable(datasetMode, datasetClipLabLoadState, datasetClipLab),
      ),
    );
  }, [
    activeClipItem,
    activeSliceSummary?.id,
    clipLabRowById,
    datasetClipLab?.invalid_state,
    datasetClipLab?.stale_state,
    datasetClipLabLoadState,
    datasetMode,
    datasetRunId,
    datasetSlicerResults,
    recordings,
    sliceMap,
  ]);

  const activeClipWaveformPeaksUrl = useMemo(() => {
    if (!activeClip?.item_metadata) {
      return null;
    }
    const peaksPath = activeClip.item_metadata.waveform_peaks_url;
    return typeof peaksPath === "string" && peaksPath.trim() ? resolveApiUrl(peaksPath) : null;
  }, [activeClip]);

  const activeClipAudioUrl = useMemo(() => {
    if (!activeClip) {
      return null;
    }
    return resolveApiUrl(activeClip.audio_url);
  }, [activeClip]);

  const handleQueueSortModeChange = useCallback((nextMode: ClipQueueSortMode) => {
    setQueueSortMode(nextMode);
  }, []);

  function handleClipSelect(nextClipItem: ClipLabItemRef) {
    setActiveClip(null);
    startTransition(() => {
      onActiveClipItemChange(nextClipItem);
    });
  }

  function getNextClipItem(currentClipItem: ClipLabItemRef): ClipLabItemRef | null {
    const visibleIds = visibleQueueClipIds;
    if (visibleIds.length === 0) {
      return null;
    }

    const currentIndex = visibleIds.findIndex((clipId) => clipId === currentClipItem.id);
    if (currentIndex < 0) {
      return visibleIds[0] ? { id: visibleIds[0] } : null;
    }

    const nextClipId = visibleIds[currentIndex + 1] ?? visibleIds[0] ?? null;
    if (!nextClipId || nextClipId === currentClipItem.id) {
      return null;
    }

    return { id: nextClipId };
  }

  function replaceSlice(updatedSlice: Slice) {
    setSlices((current) =>
      current.map((slice) => (slice.id === updatedSlice.id ? summarizeSlice(updatedSlice) : slice)),
    );
  }

  async function refreshActiveClipItem(nextClipItem: ClipLabItemRef): Promise<ClipLabItem> {
    const detail = await fetchClipLabItem(nextClipItem.id);
    setActiveClip(detail);
    return detail;
  }

  async function saveFullClipLabItem(
    clipItem: ClipLabItemRef,
    payload: {
      modified_text?: string | null;
      tags?: { name: string; color: string }[] | null;
      status?: ReviewStatus | null;
      message?: string | null;
      is_milestone?: boolean;
    },
  ): Promise<ClipLabItem> {
    if (datasetMode && datasetRunId) {
      const existingRow = datasetSlicerResults?.candidate_review_manifest.find(
        (row) => String(row.id) === clipItem.id,
      ) as DatasetCandidateClip | undefined;
      const primaryRecording = recordings[0];
      if (!existingRow || !primaryRecording) {
        throw new Error("Dataset candidate clip could not be updated.");
      }

      assertDatasetClipLabEditable(
        datasetMode,
        datasetClipLabLoadStateRef.current,
        datasetClipLabRef.current,
      );

      const currentRow = datasetClipLabRef.current?.clips.find((clip) => clip.clip_id === clipItem.id);
      if (!currentRow) {
        throw new Error("Clip Lab row is no longer available.");
      }

      const partial: Omit<
        DatasetClipLabPatchRequest,
        "expected_manifest_sha256" | "expected_clip_version"
      > = {};
      if (payload.status !== undefined && payload.status !== null) {
        partial.review_status = payload.status;
      }
      if (payload.modified_text !== undefined) {
        partial.transcript_override = payload.modified_text;
      }
      if (payload.tags !== undefined) {
        partial.reviewer_tags = (payload.tags ?? []).map((tag) => tag.name);
      }

      if (Object.keys(partial).length === 0) {
        const summary = sliceMap.get(clipItem.id);
        const qcScores: DatasetQcScores | null = summary
          ? {
              transcriptMatch: getSliceTranscriptConfidence(summary),
              speakerCheck: getSliceSpeakerPurityScore(summary),
            }
          : null;
        return buildDatasetClipLabItem(existingRow, currentRow, qcScores);
      }

      const updatedRow = await patchDatasetClipLab(clipItem.id, () => partial);
      const summary = sliceMap.get(clipItem.id);
      const qcScores: DatasetQcScores | null = summary
        ? {
            transcriptMatch: getSliceTranscriptConfidence(summary),
            speakerCheck: getSliceSpeakerPurityScore(summary),
          }
        : null;
      const updated = buildDatasetClipLabItem(existingRow, updatedRow, qcScores);
      setActiveClip(updated);
      return updated;
    }
    const updatedSlice = await saveClipState(clipItem.id, payload);
    replaceSlice(updatedSlice);
    return await refreshActiveClipItem(clipItem);
  }

  function buildDatasetClipLabItem(
    manifestRow: DatasetCandidateClip,
    clipLabRow?: DatasetClipLabClipRow,
    qcScores?: DatasetQcScores | null,
  ): ClipLabItem {
    const primaryRecording = recordings[0];
    if (!primaryRecording || !datasetRunId) {
      throw new Error("Dataset candidate clip could not be loaded.");
    }
    return datasetClipLabItem(
      manifestRow,
      primaryRecording,
      datasetRunId,
      clipLabRow,
      qcScores,
      isDatasetClipLabEditable(datasetMode, datasetClipLabLoadStateRef.current, datasetClipLabRef.current),
    );
  }

  async function saveClipEdl(
    clipItem: ClipLabItemRef,
    payload: {
      op: string;
      range?: { start_seconds: number; end_seconds: number } | null;
      duration_seconds?: number | null;
    },
  ): Promise<ClipLabItem> {
    if (datasetMode) {
      if (!datasetRunId) {
        throw new Error("Select a dataset run before editing audio.");
      }
      assertDatasetClipLabEditable(
        datasetMode,
        datasetClipLabLoadStateRef.current,
        datasetClipLabRef.current,
      );

      const manifestRow = datasetSlicerResults?.candidate_review_manifest.find(
        (row) => String(row.id) === clipItem.id,
      ) as DatasetCandidateClip | undefined;
      const currentRow = datasetClipLabRef.current?.clips.find((clip) => clip.clip_id === clipItem.id);
      if (!manifestRow || !currentRow) {
        throw new Error("Clip Lab row is no longer available.");
      }

      const sampleRateHz = requireDatasetSampleRateHz(
        currentRow.sample_rate_hz,
        manifestRow.sample_rate,
      );
      const operation = datasetAudioOperationFromEdlPayload(payload, sampleRateHz);
      const updatedRow = await patchCoordinator.appendDatasetAudioOperation(
        datasetRunId,
        clipItem.id,
        operation,
      );
      const summary = sliceMap.get(clipItem.id);
      const qcScores: DatasetQcScores | null = summary
        ? {
            transcriptMatch: getSliceTranscriptConfidence(summary),
            speakerCheck: getSliceSpeakerPurityScore(summary),
          }
        : null;
      const updated = buildDatasetClipLabItem(manifestRow, updatedRow, qcScores);
      setActiveClip(updated);
      return updated;
    }
    const updatedSlice = await appendClipEdlOperation(clipItem.id, payload);
    replaceSlice(updatedSlice);
    return await refreshActiveClipItem(clipItem);
  }

  async function undoClipMutation(clipItem: ClipLabItemRef): Promise<ClipLabItem> {
    if (datasetMode) {
      if (!datasetRunId) {
        throw new Error("Select a dataset run before editing audio.");
      }
      assertDatasetClipLabEditable(
        datasetMode,
        datasetClipLabLoadStateRef.current,
        datasetClipLabRef.current,
      );

      const manifestRow = datasetSlicerResults?.candidate_review_manifest.find(
        (row) => String(row.id) === clipItem.id,
      ) as DatasetCandidateClip | undefined;
      if (!manifestRow) {
        throw new Error("Clip Lab row is no longer available.");
      }

      const updatedRow = await patchCoordinator.undoDatasetAudioOperation(datasetRunId, clipItem.id);
      const summary = sliceMap.get(clipItem.id);
      const qcScores: DatasetQcScores | null = summary
        ? {
            transcriptMatch: getSliceTranscriptConfidence(summary),
            speakerCheck: getSliceSpeakerPurityScore(summary),
          }
        : null;
      const updated = buildDatasetClipLabItem(manifestRow, updatedRow, qcScores);
      setActiveClip(updated);
      return updated;
    }
    const updatedSlice = await undoClip(clipItem.id);
    replaceSlice(updatedSlice);
    return await refreshActiveClipItem(clipItem);
  }

  async function redoClipMutation(clipItem: ClipLabItemRef): Promise<ClipLabItem> {
    if (datasetMode) {
      if (!datasetRunId) {
        throw new Error("Select a dataset run before editing audio.");
      }
      assertDatasetClipLabEditable(
        datasetMode,
        datasetClipLabLoadStateRef.current,
        datasetClipLabRef.current,
      );

      const manifestRow = datasetSlicerResults?.candidate_review_manifest.find(
        (row) => String(row.id) === clipItem.id,
      ) as DatasetCandidateClip | undefined;
      if (!manifestRow) {
        throw new Error("Clip Lab row is no longer available.");
      }

      const updatedRow = await patchCoordinator.redoDatasetAudioOperation(datasetRunId, clipItem.id);
      const summary = sliceMap.get(clipItem.id);
      const qcScores: DatasetQcScores | null = summary
        ? {
            transcriptMatch: getSliceTranscriptConfidence(summary),
            speakerCheck: getSliceSpeakerPurityScore(summary),
          }
        : null;
      const updated = buildDatasetClipLabItem(manifestRow, updatedRow, qcScores);
      setActiveClip(updated);
      return updated;
    }
    const updatedSlice = await redoClip(clipItem.id);
    replaceSlice(updatedSlice);
    return await refreshActiveClipItem(clipItem);
  }

  async function splitClipMutation(clipItem: ClipLabItemRef, splitAtSeconds: number): Promise<number> {
    if (datasetMode) {
      throw new Error("Split is unavailable for dataset candidate clips.");
    }
    const existingIds = new Set(slices.map((slice) => slice.id));
    const nextSlices = await splitClip(clipItem.id, splitAtSeconds);
    setSlices(nextSlices);
    setActiveClip(null);
    const sorted = sortClipsForQueue(nextSlices, queueSortMode);
    const nextActiveClip = sorted.find((slice) => !existingIds.has(slice.id)) ?? sorted[0] ?? null;
    onActiveClipItemChange(nextActiveClip ? { id: nextActiveClip.id } : null);
    return nextSlices.length;
  }

  async function mergeNextClipMutation(clipItem: ClipLabItemRef): Promise<number> {
    if (datasetMode) {
      throw new Error("Merge is unavailable for dataset candidate clips.");
    }
    const existingIds = new Set(slices.map((slice) => slice.id));
    const nextSlices = await mergeWithNextClip(clipItem.id);
    setSlices(nextSlices);
    setActiveClip(null);
    const sorted = sortClipsForQueue(nextSlices, queueSortMode);
    const nextActiveClip = sorted.find((slice) => !existingIds.has(slice.id)) ?? sorted[0] ?? null;
    onActiveClipItemChange(nextActiveClip ? { id: nextActiveClip.id } : null);
    return nextSlices.length;
  }

  async function runClipLabModelMutation(
    clipItem: ClipLabItemRef,
    generatorModel: string,
  ): Promise<ClipLabItem> {
    if (datasetMode) {
      throw new Error("Processing models are unavailable for dataset candidate clips.");
    }
    const updatedSlice = await runClipLabModel(clipItem.id, generatorModel);
    replaceSlice(updatedSlice);
    return await refreshActiveClipItem(clipItem);
  }

  async function setActiveVariantMutation(variantId: string) {
    if (!activeClip) {
      return;
    }
    if (datasetMode) {
      setWorkspaceNotice("Variant switching is unavailable for dataset candidate clips.");
      return;
    }
    try {
      const updatedSlice = await setActiveVariant(activeClip.id, variantId);
      replaceSlice(updatedSlice);
      await refreshActiveClipItem({ id: activeClip.id });
      setWorkspaceNotice(`Activated variant ${variantId}.`);
    } catch (error) {
      setWorkspaceNotice(getErrorMessage(error, "Variant switch failed."));
    }
  }

  async function handleMarkReferenceClipCandidate(clipItem: ClipLabItemRef, transcriptText: string) {
    if (!activeProject?.id || !datasetRunId) {
      throw new Error("Select a dataset run before marking reference clip candidates.");
    }

    const result = await markDatasetClipAsReferenceCandidate(activeProject.id, datasetRunId, {
      clip_id: clipItem.id,
      transcript_text: transcriptText,
    });
    setWorkspaceNotice(`Saved reference clip candidate: ${result.filename}`);
  }

  async function handleSaveAsReference(options?: {
    name?: string | null;
    mood_label?: string | null;
  }) {
    if (!activeClip) {
      return;
    }
    if (datasetMode) {
      setWorkspaceNotice("Saving dataset candidate clips as references is not wired yet.");
      return;
    }
    if (!activeClip.active_variant?.id) {
      setWorkspaceNotice("This slice does not have an active variant to save.");
      return;
    }

    setIsSavingReference(true);
    try {
      const reference = await saveCurrentSliceAsReference({
        slice_id: activeClip.id,
        name: options?.name ?? null,
        mood_label: options?.mood_label ?? null,
      });
      if (activeProject) {
        setReferenceAssets(await fetchProjectReferenceAssets(activeProject.id));
      }
      setWorkspaceNotice(`Saved reference: ${reference.name}.`);
    } catch (error) {
      setWorkspaceNotice(getErrorMessage(error, "Saving this slice as a reference failed."));
    } finally {
      setIsSavingReference(false);
    }
  }

  function openReferenceAssetInLibrary(assetId: string) {
    if (!activeProject) {
      return;
    }
    const url = new URL(window.location.href);
    url.pathname = "/reference";
    url.searchParams.set("project", activeProject.id);
    url.searchParams.set("asset", assetId);
    window.history.pushState({}, "", url);
    window.dispatchEvent(new PopStateEvent("popstate"));
  }

  const datasetStatusCounts = useMemo(() => {
    const counts: Record<ReviewStatus, number> = {
      unresolved: 0,
      quarantined: 0,
      accepted: 0,
      rejected: 0,
    };
    const durations: Record<ReviewStatus, number> = {
      unresolved: 0,
      quarantined: 0,
      accepted: 0,
      rejected: 0,
    };

    for (const slice of sortClipsForQueue(slices, queueSortMode)) {
      counts[slice.status] += 1;
      durations[slice.status] += getSliceDuration(slice);
    }

    return {
      counts,
      durations,
    };
  }, [slices]);

  const totalDurationSeconds = useMemo(
    () => sortClipsForQueue(slices, queueSortMode).reduce((sum, slice) => sum + getSliceDuration(slice), 0),
    [slices, queueSortMode],
  );
  const acceptedRejectedRatio =
    datasetStatusCounts.durations.rejected > 0
      ? datasetStatusCounts.durations.accepted / datasetStatusCounts.durations.rejected
      : datasetStatusCounts.durations.accepted > 0
        ? datasetStatusCounts.durations.accepted
        : null;
  const predictedOutputSeconds =
    datasetStatusCounts.counts.accepted > 0 ? datasetStatusCounts.durations.accepted : null;
  const progressPercent =
    slices.length > 0
      ? ((datasetStatusCounts.counts.accepted + datasetStatusCounts.counts.rejected) / slices.length) * 100
      : null;
  const canUndo = Boolean(activeClip?.can_undo);
  const canRedo = Boolean(activeClip?.can_redo);
  const activeCommitId = activeClip?.active_commit?.id ?? null;
  const existingReferenceForCurrentState = useMemo(() => {
    if (!activeCommitId || !activeClip) {
      return null;
    }
    return (
      referenceAssets.find(
        (asset) =>
          asset.source_slice_id === activeClip.id
          && asset.source_edit_commit_id === activeCommitId,
      ) ?? null
    );
  }, [activeClip, activeCommitId, referenceAssets]);

  useEffect(() => {
    onHeaderActionsChange(null);
    return () => {
      onHeaderActionsChange(null);
    };
  }, [onHeaderActionsChange]);

  const datasetClipLabEditable = isDatasetClipLabEditable(datasetMode, datasetClipLabLoadState, datasetClipLab);
  const activeClipLabRow =
    datasetClipLabEditable && activeClipItem ? clipLabRowById.get(activeClipItem.id) : undefined;
  const datasetTagComposer = activeClipLabRow
    ? {
        reviewStatus: activeClipLabRow.review_status,
        machineFindings: activeClipLabRow.pipeline_findings,
        reviewerTags: activeClipLabRow.reviewer_tags,
        acceptanceStale: activeClipLabRow.acceptance_stale,
        onReviewStatusChange: (status: ReviewStatus) =>
          patchDatasetClipLab(activeClipLabRow.clip_id, () => ({ review_status: status })),
        onAddReviewerTag: (tag: string) =>
          patchDatasetClipLab(activeClipLabRow.clip_id, (row) => ({
            reviewer_tags: [...row.reviewer_tags, tag],
          })),
        onRemoveReviewerTag: (tag: string) =>
          patchDatasetClipLab(activeClipLabRow.clip_id, (row) => ({
            reviewer_tags: row.reviewer_tags.filter(
              (entry) => entry.toLowerCase() !== tag.toLowerCase(),
            ),
          })),
      }
    : undefined;
  const datasetTagReadOnly = buildDatasetTagReadOnlyConfig(
    datasetMode,
    datasetClipLabEditable,
    activeClip,
    datasetClipLab,
    datasetClipLabLoadState,
  );

  return (
    <ErrorBoundary
      resetKey={activeProject?.id ?? "no-project"}
      fallback={
        <WorkspaceStatePanel
          title="Label workstation crashed"
          message="The labeling UI hit a render error. Reload the workspace to recover."
          actionLabel="Retry load"
          onAction={() => void loadWorkspace(activeProject?.id)}
        />
      }
    >
      <div className="workspace-shell">
        {workspaceNotice ? <p className="workspace-notice">{workspaceNotice}</p> : null}
        {datasetClipLabLoadState === "unavailable" ? (
          <p className="workspace-notice workspace-warning" role="alert">
            Clip Lab state unavailable. Reload the workspace before editing tags, status, or transcript.
          </p>
        ) : null}
        {datasetClipLab?.invalid_state ? (
          <p className="workspace-notice workspace-warning" role="alert">
            Clip Lab state is invalid
            {datasetClipLab.invalid_state_reason ? `: ${datasetClipLab.invalid_state_reason}` : "."}
          </p>
        ) : null}
        {datasetClipLab?.stale_state ? (
          <p className="workspace-notice workspace-warning" role="alert">
            Clip Lab state is stale
            {datasetClipLab.stale_reason ? `: ${datasetClipLab.stale_reason}` : "."}
          </p>
        ) : null}

        {workspaceStatus === "error" && !activeProject ? (
          <WorkspaceStatePanel
            title="Project list unavailable"
            message={workspaceError ?? projectLoadError ?? "The backend could not load projects."}
            actionLabel="Retry projects"
            onAction={onRetryProjects}
          />
        ) : null}

        <div className="workspace-grid">
          <ClipQueuePane
            workspacePhase={workspaceStatus}
            workspaceError={workspaceError}
            workspaceEmptyMessage={workspaceEmptyMessage}
            recordings={recordings}
            clips={queueSlices}
            activeClipItem={activeClipItem}
            sortMode={queueSortMode}
            onSortModeChange={handleQueueSortModeChange}
            onSelectClipItem={handleClipSelect}
            onRetryLoad={() => void loadWorkspace(activeProject?.id)}
            onVisibleClipIdsChange={setVisibleQueueClipIds}
          />

          <EditorPane
            workspacePhase={workspaceStatus}
            workspaceError={workspaceError}
            workspaceEmptyMessage={workspaceEmptyMessage}
            activeClip={activeClip}
            waveformPeaksUrl={datasetMode ? activeClipWaveformPeaksUrl : null}
            activeClipAudioUrl={activeClipAudioUrl}
            canUndo={canUndo}
            canRedo={canRedo}
            allClipTagNames={allClipTagNames}
            getNextClipItem={getNextClipItem}
            onSelectClip={handleClipSelect}
            onRetryLoad={() => void loadWorkspace(activeProject?.id)}
            onSaveClipLabItem={saveFullClipLabItem}
            onAppendEdlOperation={saveClipEdl}
            onUndo={undoClipMutation}
            onRedo={redoClipMutation}
            onSplitClip={splitClipMutation}
            onMergeClip={mergeNextClipMutation}
            onRunClipLabModel={runClipLabModelMutation}
            onMarkReferenceClipCandidate={datasetRunId ? handleMarkReferenceClipCandidate : undefined}
            datasetTagComposer={datasetTagComposer}
            datasetTagReadOnly={datasetTagReadOnly}
            datasetEditingDisabled={datasetMode && !datasetClipLabEditable}
          />

          <InspectorPane
            workspacePhase={workspaceStatus}
            workspaceError={workspaceError}
            activeClip={activeClip}
            datasetRuns={datasetRuns}
            selectedDatasetRunId={datasetRunId}
            onDatasetRunChange={handleLabDatasetRunChange}
            totalClipCount={sortClipsForQueue(slices, queueSortMode).length}
            totalDurationSeconds={totalDurationSeconds}
            datasetStatusCounts={datasetStatusCounts}
            acceptedRejectedRatio={acceptedRejectedRatio}
            predictedOutputSeconds={predictedOutputSeconds}
            progressPercent={progressPercent}
            exportRuns={exportRuns}
            onRetryLoad={() => void loadWorkspace(activeProject?.id)}
            onStatusChange={(status) => {
              if (!activeClip) {
                return;
              }
              void saveFullClipLabItem({ id: activeClip.id }, {
                status,
                message: `Status: ${status}`,
              });
            }}
            onVariantSelect={(variantId) => void setActiveVariantMutation(variantId)}
            existingReferenceForCurrentState={existingReferenceForCurrentState}
            onOpenExistingReference={openReferenceAssetInLibrary}
            onSaveAsReference={(options) => void handleSaveAsReference(options)}
            isSavingReference={isSavingReference}
          />
        </div>
      </div>
    </ErrorBoundary>
  );
}
