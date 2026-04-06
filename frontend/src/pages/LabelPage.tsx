import { startTransition, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import {
  ApiError,
  appendClipEdlOperation,
  cleanupProjectMedia,
  fetchClipLabItem,
  fetchProjectExports,
  fetchProjectRecordings,
  fetchProjectSlices,
  mergeWithNextClip,
  redoClip,
  resolveApiUrl,
  runClipLabModel,
  runProjectExport,
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
import {
  getSliceDuration,
  sortClipsForQueue,
} from "../workspace/workspace-helpers";
import type {
  ClipLabItem,
  ClipLabItemRef,
  ExportRun,
  Project,
  ReviewStatus,
  Slice,
  SliceSummary,
  SourceRecordingQueue,
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

export default function LabelPage({
  activeProject,
  activeClipItem,
  projectLoadStatus,
  projectLoadError,
  onActiveClipItemChange,
  onRetryProjects,
  onHeaderActionsChange,
}: LabelPageProps) {
  const [workspaceStatus, setWorkspaceStatus] = useState<WorkspaceStatus>("loading");
  const [workspaceError, setWorkspaceError] = useState<string | null>(null);
  const [workspaceEmptyMessage, setWorkspaceEmptyMessage] = useState<string | null>(null);
  const [workspaceNotice, setWorkspaceNotice] = useState<string | null>(null);
  const [slices, setSlices] = useState<SliceSummary[]>([]);
  const [recordings, setRecordings] = useState<SourceRecordingQueue[]>([]);
  const [activeClip, setActiveClip] = useState<ClipLabItem | null>(null);
  const [visibleQueueClipIds, setVisibleQueueClipIds] = useState<string[]>([]);
  const [exportRuns, setExportRuns] = useState<ExportRun[]>([]);
  const [isRunningExport, setIsRunningExport] = useState(false);
  const [isCleaningMedia, setIsCleaningMedia] = useState(false);
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
      onActiveClipItemChange(null);
      setVisibleQueueClipIds([]);
      setWorkspaceStatus("ready");
      setWorkspaceEmptyMessage("Select a project to open the review workstation.");
      return;
    }

    try {
      const [nextSlices, nextRecordings, nextExports] = await Promise.all([
        fetchProjectSlices(projectId),
        fetchProjectRecordings(projectId),
        fetchProjectExports(projectId),
      ]);

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
  }, [activeProject?.id, projectLoadStatus, projectLoadError]);

  useEffect(() => {
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
  }, [activeProject?.id, recordings, workspaceStatus]);

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

  const activeSliceSummary = useMemo(() => {
    const visibleQueueClips = visibleQueueClipIds
      .map((clipId) => sliceMap.get(clipId) ?? null)
      .filter((slice): slice is SliceSummary => slice !== null);

    if (activeClipItem) {
      return sliceMap.get(activeClipItem.id) ?? visibleQueueClips[0] ?? slices[0] ?? null;
    }

    return visibleQueueClips[0] ?? slices[0] ?? null;
  }, [sliceMap, slices, activeClipItem, visibleQueueClipIds]);

  useEffect(() => {
    const requestId = latestDetailRequestRef.current + 1;
    latestDetailRequestRef.current = requestId;

    const nextActiveTarget = activeClipItem ?? (activeSliceSummary ? { id: activeSliceSummary.id } : null);

    if (!nextActiveTarget) {
      setActiveClip(null);
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
  }, [activeClipItem, activeSliceSummary?.id]);

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
    const updatedSlice = await appendClipEdlOperation(clipItem.id, payload);
    replaceSlice(updatedSlice);
    return await refreshActiveClipItem(clipItem);
  }

  async function undoClipMutation(clipItem: ClipLabItemRef): Promise<ClipLabItem> {
    const updatedSlice = await undoClip(clipItem.id);
    replaceSlice(updatedSlice);
    return await refreshActiveClipItem(clipItem);
  }

  async function redoClipMutation(clipItem: ClipLabItemRef): Promise<ClipLabItem> {
    const updatedSlice = await redoClip(clipItem.id);
    replaceSlice(updatedSlice);
    return await refreshActiveClipItem(clipItem);
  }

  async function splitClipMutation(clipItem: ClipLabItemRef, splitAtSeconds: number): Promise<number> {
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
    const updatedSlice = await runClipLabModel(clipItem.id, generatorModel);
    replaceSlice(updatedSlice);
    return await refreshActiveClipItem(clipItem);
  }

  async function setActiveVariantMutation(variantId: string) {
    if (!activeClip) {
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

  useEffect(() => {
    onHeaderActionsChange(
      activeProject ? (
        <>
          {showDangerousDevActions ? (
            <button type="button" onClick={() => void handleCleanupMedia()} disabled={isCleaningMedia}>
              {isCleaningMedia ? "Cleaning media..." : "Cleanup Media"}
            </button>
          ) : null}
          <button className="primary-button" type="button" onClick={() => void handleRunExport()} disabled={isRunningExport}>
            {isRunningExport ? "Running export..." : "Run Export"}
          </button>
        </>
      ) : null,
    );

    return () => {
      onHeaderActionsChange(null);
    };
  }, [activeProject, isCleaningMedia, isRunningExport, onHeaderActionsChange, showDangerousDevActions]);

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
            clips={slices}
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
          />
        </div>
      </div>
    </ErrorBoundary>
  );
}
