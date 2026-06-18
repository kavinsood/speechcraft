import { startTransition, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import {
  ApiError,
  appendClipEdlOperation,
  buildCandidateReviewAudioUrl,
  cleanupProjectMedia,
  fetchProjectReferenceAssets,
  fetchClipLabItem,
  fetchDatasetSlicerResults,
  fetchProjectExports,
  fetchProjectRecordings,
  fetchProjectSlices,
  mergeWithNextClip,
  redoClip,
  resolveApiUrl,
  runClipLabModel,
  runProjectExport,
  saveCurrentSliceAsReference,
  saveClipState,
  setActiveVariant,
  splitClip,
  undoClip,
} from "../api";
import ErrorBoundary from "../ErrorBoundary";
import ClipQueuePane from "../workspace/ClipQueuePane";
import EditorPane from "../workspace/EditorPane";
import InspectorPane from "../workspace/InspectorPane";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";
import { usePipelineContext } from "../pipeline/PipelineContext";
import {
  getSliceDuration,
  sortClipsForQueue,
} from "../workspace/workspace-helpers";
import type {
  ClipLabItem,
  ClipLabItemRef,
  ClipLabCapabilities,
  ClipLabCommit,
  ClipLabTranscript,
  ClipLabVariant,
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

const ACTIVE_RECORDING_PROCESSING_STATES = new Set(["transcribing", "aligning", "slicing"]);

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
type DatasetClipLocalState = {
  status: ReviewStatus;
  modifiedText: string | null;
  tags: Tag[];
  message?: string | null;
};

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

function candidateReviewTags(row: DatasetCandidateClip, overrideTags?: Tag[] | null): Tag[] {
  if (overrideTags) {
    return overrideTags;
  }
  const reasonCodes = Array.isArray(row.review_reason_codes) ? row.review_reason_codes : [];
  return reasonCodes.map((reason, index) => ({
    id: `${String(row.id)}-tag-${index}`,
    name: String(reason).replace(/_/g, " "),
    color: "#7c3aed",
  }));
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

function datasetCapabilities(): ClipLabCapabilities {
  return {
    can_edit_transcript: true,
    can_edit_tags: true,
    can_set_status: true,
    can_save: true,
    can_split: false,
    can_merge: false,
    can_edit_waveform: false,
    can_run_processing: false,
    can_switch_variants: false,
    can_export: false,
    can_finalize: false,
  };
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

function datasetSliceSummary(
  row: DatasetCandidateClip,
  sourceRecordingId: string,
  localState?: DatasetClipLocalState,
): SliceSummary {
  const tags = candidateReviewTags(row, localState?.tags ?? null);
  const transcript = datasetTranscriptSummary(row, localState?.modifiedText ?? null);
  return {
    id: String(row.id),
    source_recording_id: sourceRecordingId,
    active_variant_id: `${String(row.id)}:variant:analysis`,
    active_commit_id: `${String(row.id)}:commit:current`,
    status: localState?.status ?? "unresolved",
    is_locked: false,
    duration_seconds: Number(row.duration_sec ?? 0),
    model_metadata: {
      candidate_review: true,
      source_audio_id: row.source_audio_id ?? null,
      start_cutpoint_ref: row.start_cutpoint_ref ?? null,
      end_cutpoint_ref: row.end_cutpoint_ref ?? null,
      needs_review: Boolean(row.needs_review),
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
  localState?: DatasetClipLocalState,
): ClipLabItem {
  const tags = candidateReviewTags(row, localState?.tags ?? null);
  const transcriptText = localState?.modifiedText ?? String(row.training_text ?? "");
  const status = localState?.status ?? "unresolved";
  const variant = datasetVariant(row);
  const transcript = datasetTranscript(row, localState?.modifiedText ?? null);
  const commit = datasetCommit(row, transcriptText, status, tags, localState?.message ?? "Dataset candidate clip");
  return {
    id: String(row.id),
    kind: "slice",
    source_recording_id: sourceRecording.id,
    source_recording: sourceRecording,
    start_seconds: Number(row.source_start_sample ?? 0) / Number(row.sample_rate ?? 16000),
    end_seconds: Number(row.source_end_sample ?? 0) / Number(row.sample_rate ?? 16000),
    duration_seconds: Number(row.duration_sec ?? 0),
    status,
    is_locked: false,
    created_at: new Date(0).toISOString(),
    transcript,
    tags,
    speaker_name: null,
    language: "en",
    audio_url: buildCandidateReviewAudioUrl(runId, String(row.id)),
    item_metadata: {
      candidate_review: true,
      review_reason_codes: Array.isArray(row.review_reason_codes) ? row.review_reason_codes : [],
      buffer_warning_reason_codes: Array.isArray(row.buffer_warning_reason_codes) ? row.buffer_warning_reason_codes : [],
      source_start_sample: row.source_start_sample ?? null,
      source_end_sample: row.source_end_sample ?? null,
    },
    transcript_source: "dataset_candidate",
    can_run_asr: false,
    asr_placeholder_message: "Candidate review clips are already aligned and sliced.",
    active_variant_generator_model: null,
    can_undo: false,
    can_redo: false,
    capabilities: datasetCapabilities(),
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
  const { selectedLabDatasetRunId, selectedSlicerDatasetRunId } = usePipelineContext();
  const datasetRunId = selectedLabDatasetRunId ?? selectedSlicerDatasetRunId;
  const datasetMode = Boolean(datasetRunId);
  const [workspaceStatus, setWorkspaceStatus] = useState<WorkspaceStatus>("loading");
  const [workspaceError, setWorkspaceError] = useState<string | null>(null);
  const [workspaceEmptyMessage, setWorkspaceEmptyMessage] = useState<string | null>(null);
  const [workspaceNotice, setWorkspaceNotice] = useState<string | null>(null);
  const [slices, setSlices] = useState<SliceSummary[]>([]);
  const [recordings, setRecordings] = useState<SourceRecordingQueue[]>([]);
  const [activeClip, setActiveClip] = useState<ClipLabItem | null>(null);
  const [visibleQueueClipIds, setVisibleQueueClipIds] = useState<string[]>([]);
  const [exportRuns, setExportRuns] = useState<ExportRun[]>([]);
  const [referenceAssets, setReferenceAssets] = useState<ReferenceAssetSummary[]>([]);
  const [datasetSlicerResults, setDatasetSlicerResults] = useState<DatasetSlicerResults | null>(null);
  const [datasetClipState, setDatasetClipState] = useState<Record<string, DatasetClipLocalState>>({});
  const [isRunningExport, setIsRunningExport] = useState(false);
  const [isCleaningMedia, setIsCleaningMedia] = useState(false);
  const [isSavingReference, setIsSavingReference] = useState(false);
  const latestWorkspaceRequestRef = useRef(0);
  const latestDetailRequestRef = useRef(0);
  const showDangerousDevActions = import.meta.env.DEV;

  function getWorkspaceEmptyMessage(nextSlices: SliceSummary[], nextRecordings: SourceRecordingQueue[]): string | null {
    if (nextSlices.length > 0) {
      return null;
    }

    const activeRecording = nextRecordings.find((recording) =>
      ACTIVE_RECORDING_PROCESSING_STATES.has(recording.processing_state),
    );
    if (activeRecording) {
      return activeRecording.processing_message ?? "Generating slices from source recordings...";
    }

    const staleRecording = nextRecordings.find((recording) => recording.processing_state === "alignment_stale");
    if (staleRecording) {
      return staleRecording.processing_message ?? "Source transcript changed. Re-run alignment before slicing.";
    }

    const failedRecording = nextRecordings.find((recording) => recording.processing_state === "failed");
    if (failedRecording) {
      return failedRecording.processing_message ?? "Recording processing failed. Check the backend logs.";
    }

    if (nextRecordings.length > 0) {
      return "This project has recordings, but no slices yet.";
    }

    return "This project does not contain slices yet.";
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
      onActiveClipItemChange(null);
      setVisibleQueueClipIds([]);
      setWorkspaceStatus("ready");
      setWorkspaceEmptyMessage("Select a project to open the review workstation.");
      return;
    }

    try {
      if (datasetMode && datasetRunId) {
        const [results, nextRecordings, nextExports] = await Promise.all([
          fetchDatasetSlicerResults(datasetRunId),
          fetchProjectRecordings(projectId),
          fetchProjectExports(projectId),
        ]);
        let nextReferenceAssets: ReferenceAssetSummary[] = [];
        try {
          nextReferenceAssets = await fetchProjectReferenceAssets(projectId);
        } catch (error) {
          if (latestWorkspaceRequestRef.current === requestId) {
            setWorkspaceNotice(
              getErrorMessage(error, "The reference library did not load, so duplicate-save protection is unavailable right now."),
            );
          }
        }
        if (latestWorkspaceRequestRef.current !== requestId) {
          return;
        }
        const primaryRecording = nextRecordings[0] ?? null;
        const nextSlices = primaryRecording
          ? results.candidate_review_manifest.map((row) =>
              datasetSliceSummary(
                row,
                primaryRecording.id,
                datasetClipState[String(row.id)],
              ),
            )
          : [];
        const sortedSlices = sortClipsForQueue(nextSlices);
        setDatasetSlicerResults(results);
        setSlices(nextSlices);
        setRecordings(nextRecordings);
        if (!options?.silent) {
          setActiveClip(null);
        }
        setExportRuns(nextExports);
        setReferenceAssets(nextReferenceAssets);
        setVisibleQueueClipIds(sortedSlices.map((slice) => slice.id));
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
        return;
      }

      const [nextSlices, nextRecordings, nextExports] = await Promise.all([
        fetchProjectSlices(projectId),
        fetchProjectRecordings(projectId),
        fetchProjectExports(projectId),
      ]);
      let nextReferenceAssets: ReferenceAssetSummary[] = [];

      try {
        nextReferenceAssets = await fetchProjectReferenceAssets(projectId);
      } catch (error) {
        if (latestWorkspaceRequestRef.current === requestId) {
          setWorkspaceNotice(
            getErrorMessage(error, "The reference library did not load, so duplicate-save protection is unavailable right now."),
          );
        }
      }

      if (latestWorkspaceRequestRef.current !== requestId) {
        return;
      }

      const sortedSlices = sortClipsForQueue(nextSlices);
      setSlices(nextSlices);
      setRecordings(nextRecordings);
      if (!options?.silent) {
        setActiveClip(null);
      }
      setExportRuns(nextExports);
      setReferenceAssets(nextReferenceAssets);
      setVisibleQueueClipIds(sortedSlices.map((slice) => slice.id));
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
      setWorkspaceEmptyMessage(getWorkspaceEmptyMessage(sortedSlices, nextRecordings));
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

  useEffect(() => {
    if (datasetMode) {
      return;
    }
    if (!activeProject?.id || workspaceStatus !== "ready") {
      return;
    }
    if (!recordings.some((recording) => ACTIVE_RECORDING_PROCESSING_STATES.has(recording.processing_state))) {
      return;
    }

    const intervalId = window.setInterval(() => {
      void loadWorkspace(activeProject.id, { silent: true });
    }, 3000);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [activeProject?.id, recordings, workspaceStatus, datasetMode]);

  const allClipTagNames = useMemo(() => {
    return Array.from(
      new Set(
        slices
          .flatMap((slice) => slice.tags)
          .map((tag) => tag.name.toLowerCase())
          .sort(),
      ),
    );
  }, [slices]);

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

    if (datasetMode && datasetRunId) {
      const primaryRecording = recordings[0] ?? null;
      const candidateRow = datasetSlicerResults?.candidate_review_manifest.find(
        (row) => String(row.id) === nextActiveTarget.id,
      ) as DatasetCandidateClip | undefined;
      if (!primaryRecording || !candidateRow) {
        setActiveClip(null);
        setWorkspaceNotice("Dataset candidate clip could not be loaded.");
        return;
      }
      setActiveClip(
        datasetClipLabItem(
          candidateRow,
          primaryRecording,
          datasetRunId,
          datasetClipState[nextActiveTarget.id],
        ),
      );
      return;
    }

    void (async () => {
      try {
        const detail = await fetchClipLabItem(nextActiveTarget.id);
        if (latestDetailRequestRef.current !== requestId) {
          return;
        }
        setActiveClip(detail);
      } catch (error) {
        if (latestDetailRequestRef.current !== requestId) {
          return;
        }
        setActiveClip(null);
        setWorkspaceNotice(getErrorMessage(error, "The active Clip Lab item failed to load."));
      }
    })();
  }, [activeClipItem, activeSliceSummary?.id, datasetMode, datasetRunId, datasetSlicerResults, recordings, datasetClipState]);

  const activeClipAudioUrl = useMemo(() => {
    if (!activeClip) {
      return null;
    }
    return resolveApiUrl(activeClip.audio_url);
  }, [activeClip]);

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

  async function handleRunExport() {
    if (!activeProject) {
      return;
    }

    setIsRunningExport(true);
    try {
      const result = await runProjectExport(activeProject.id);
      const [nextSlices, nextExports] = await Promise.all([
        fetchProjectSlices(activeProject.id),
        fetchProjectExports(activeProject.id),
      ]);
      setSlices(nextSlices);
      setExportRuns(nextExports);
      setWorkspaceNotice(`Export completed: ${result.accepted_clip_count} accepted slice(s) rendered.`);
    } catch (error) {
      setWorkspaceNotice(getErrorMessage(error, "Export failed. See backend logs for details."));
    } finally {
      setIsRunningExport(false);
    }
  }

  async function handleCleanupMedia() {
    if (!activeProject) {
      return;
    }
    if (
      !window.confirm(
        "Cleanup removes superseded slices and unreferenced media files for this project. Continue?",
      )
    ) {
      return;
    }

    setIsCleaningMedia(true);
    try {
      const result = await cleanupProjectMedia(activeProject.id);
      const nextSlices = await fetchProjectSlices(activeProject.id);
      const sorted = sortClipsForQueue(nextSlices);
      setSlices(nextSlices);
      setActiveClip(null);
      setVisibleQueueClipIds(sorted.map((slice) => slice.id));
      onActiveClipItemChange(sorted[0] ? { id: sorted[0].id } : null);
      setWorkspaceNotice(
        `Cleanup removed ${result.deleted_slice_count} superseded slice(s), ${result.deleted_variant_count} variant row(s), and ${result.deleted_file_count} file(s).`,
      );
    } catch (error) {
      setWorkspaceNotice(getErrorMessage(error, "Project media cleanup failed."));
    } finally {
      setIsCleaningMedia(false);
    }
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
      const existingRow = datasetSlicerResults?.candidate_review_manifest.find((row) => String(row.id) === clipItem.id) as DatasetCandidateClip | undefined;
      const primaryRecording = recordings[0];
      if (!existingRow || !primaryRecording) {
        throw new Error("Dataset candidate clip could not be updated.");
      }
      const nextState: DatasetClipLocalState = {
        status: payload.status ?? datasetClipState[clipItem.id]?.status ?? "unresolved",
        modifiedText:
          payload.modified_text !== undefined
            ? payload.modified_text
            : datasetClipState[clipItem.id]?.modifiedText ?? null,
        tags:
          payload.tags !== undefined
            ? (payload.tags ?? []).map((tag, index) => ({
                id: `${clipItem.id}-tag-${index}`,
                name: tag.name,
                color: tag.color,
              }))
            : datasetClipState[clipItem.id]?.tags ?? candidateReviewTags(existingRow),
        message: payload.message ?? datasetClipState[clipItem.id]?.message ?? null,
      };
      setDatasetClipState((current) => ({ ...current, [clipItem.id]: nextState }));
      const updated = datasetClipLabItem(existingRow, primaryRecording, datasetRunId, nextState);
      replaceSlice(updated as unknown as Slice);
      setActiveClip(updated);
      return updated;
    }
    const updatedSlice = await saveClipState(clipItem.id, payload);
    replaceSlice(updatedSlice);
    return await refreshActiveClipItem(clipItem);
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
      throw new Error("Waveform edits are unavailable for dataset candidate clips.");
    }
    const updatedSlice = await appendClipEdlOperation(clipItem.id, payload);
    replaceSlice(updatedSlice);
    return await refreshActiveClipItem(clipItem);
  }

  async function undoClipMutation(clipItem: ClipLabItemRef): Promise<ClipLabItem> {
    if (datasetMode) {
      throw new Error("Undo is unavailable for dataset candidate clips.");
    }
    const updatedSlice = await undoClip(clipItem.id);
    replaceSlice(updatedSlice);
    return await refreshActiveClipItem(clipItem);
  }

  async function redoClipMutation(clipItem: ClipLabItemRef): Promise<ClipLabItem> {
    if (datasetMode) {
      throw new Error("Redo is unavailable for dataset candidate clips.");
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
    const sorted = sortClipsForQueue(nextSlices);
    setVisibleQueueClipIds(sorted.map((slice) => slice.id));
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
    const sorted = sortClipsForQueue(nextSlices);
    setVisibleQueueClipIds(sorted.map((slice) => slice.id));
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

    for (const slice of sortClipsForQueue(slices)) {
      counts[slice.status] += 1;
      durations[slice.status] += getSliceDuration(slice);
    }

    return {
      counts,
      durations,
    };
  }, [slices]);

  const totalDurationSeconds = useMemo(
    () => sortClipsForQueue(slices).reduce((sum, slice) => sum + getSliceDuration(slice), 0),
    [slices],
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
    onHeaderActionsChange(
      activeProject ? (
        <>
          {!datasetMode && showDangerousDevActions ? (
            <button type="button" onClick={() => void handleCleanupMedia()} disabled={isCleaningMedia}>
              {isCleaningMedia ? "Cleaning media..." : "Cleanup Media"}
            </button>
          ) : null}
          {!datasetMode ? (
            <button className="primary-button" type="button" onClick={() => void handleRunExport()} disabled={isRunningExport}>
              {isRunningExport ? "Running export..." : "Run Export"}
            </button>
          ) : null}
        </>
      ) : null,
    );

    return () => {
      onHeaderActionsChange(null);
    };
  }, [activeProject, isCleaningMedia, isRunningExport, onHeaderActionsChange, showDangerousDevActions, datasetMode]);

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
            onSelectClipItem={handleClipSelect}
            onRetryLoad={() => void loadWorkspace(activeProject?.id)}
            onVisibleClipIdsChange={setVisibleQueueClipIds}
          />

          <EditorPane
            workspacePhase={workspaceStatus}
            workspaceError={workspaceError}
            workspaceEmptyMessage={workspaceEmptyMessage}
            activeClip={activeClip}
            disableWaveformPeaks={datasetMode}
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
          />

          <InspectorPane
            workspacePhase={workspaceStatus}
            workspaceError={workspaceError}
            activeClip={activeClip}
            totalClipCount={sortClipsForQueue(slices).length}
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
