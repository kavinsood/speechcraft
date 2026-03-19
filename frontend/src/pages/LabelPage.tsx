import { startTransition, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import {
  ApiError,
  appendClipEdlOperation,
  buildSliceAudioUrl,
  cleanupProjectMedia,
  fetchProjectExports,
  fetchProjectSlices,
  mergeWithNextClip,
  redoClip,
  runClipLabModel,
  runProjectExport,
  setActiveVariant,
  splitClip,
  undoClip,
  updateClipStatus,
  updateClipTags,
  updateClipTranscript,
} from "../api";
import ErrorBoundary from "../ErrorBoundary";
import ClipQueuePane from "../workspace/ClipQueuePane";
import EditorPane from "../workspace/EditorPane";
import InspectorPane from "../workspace/InspectorPane";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";
import {
  getRedoTarget,
  getSliceDuration,
  sortClipsForQueue,
} from "../workspace/workspace-helpers";
import type { ExportRun, Project, ReviewStatus, Slice } from "../types";

type WorkspaceStatus = "loading" | "error" | "ready";

type LabelPageProps = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
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

export default function LabelPage({
  activeProject,
  projectLoadStatus,
  projectLoadError,
  onRetryProjects,
  onHeaderActionsChange,
}: LabelPageProps) {
  const [workspaceStatus, setWorkspaceStatus] = useState<WorkspaceStatus>("loading");
  const [workspaceError, setWorkspaceError] = useState<string | null>(null);
  const [workspaceEmptyMessage, setWorkspaceEmptyMessage] = useState<string | null>(null);
  const [workspaceNotice, setWorkspaceNotice] = useState<string | null>(null);
  const [slices, setSlices] = useState<Slice[]>([]);
  const [activeClipId, setActiveClipId] = useState<string | null>(null);
  const [visibleQueueClipIds, setVisibleQueueClipIds] = useState<string[]>([]);
  const [exportRuns, setExportRuns] = useState<ExportRun[]>([]);
  const [isRunningExport, setIsRunningExport] = useState(false);
  const [isCleaningMedia, setIsCleaningMedia] = useState(false);
  const latestWorkspaceRequestRef = useRef(0);
  const showDangerousDevActions = import.meta.env.DEV;

  async function loadWorkspace(projectId: string | null | undefined) {
    const requestId = latestWorkspaceRequestRef.current + 1;
    latestWorkspaceRequestRef.current = requestId;
    setWorkspaceStatus("loading");
    setWorkspaceError(null);
    setWorkspaceEmptyMessage(null);
    setWorkspaceNotice(null);

    if (!projectId) {
      setSlices([]);
      setExportRuns([]);
      setActiveClipId(null);
      setVisibleQueueClipIds([]);
      setWorkspaceStatus("ready");
      setWorkspaceEmptyMessage("Select a project to open the review workstation.");
      return;
    }

    try {
      const [nextSlices, nextExports] = await Promise.all([
        fetchProjectSlices(projectId),
        fetchProjectExports(projectId),
      ]);

      if (latestWorkspaceRequestRef.current !== requestId) {
        return;
      }

      const sortedSlices = sortClipsForQueue(nextSlices);
      setSlices(nextSlices);
      setExportRuns(nextExports);
      setVisibleQueueClipIds(sortedSlices.map((slice) => slice.id));
      setActiveClipId((current) =>
        sortedSlices.some((slice) => slice.id === current) ? current : (sortedSlices[0]?.id ?? null),
      );
      setWorkspaceStatus("ready");
      setWorkspaceEmptyMessage(sortedSlices.length === 0 ? "This project does not contain slices yet." : null);
    } catch (error) {
      if (latestWorkspaceRequestRef.current !== requestId) {
        return;
      }

      setSlices([]);
      setExportRuns([]);
      setActiveClipId(null);
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

  const allClipTagNames = useMemo(() => {
    return Array.from(
      new Set(
        slices
          .flatMap((slice) => slice.tags.map((tag) => tag.name.toLowerCase()))
          .sort(),
      ),
    );
  }, [slices]);

  const activeClip = useMemo(() => {
    const visibleQueueClips = visibleQueueClipIds
      .map((clipId) => slices.find((slice) => slice.id === clipId) ?? null)
      .filter((slice): slice is Slice => slice !== null);

    return slices.find((slice) => slice.id === activeClipId) ?? visibleQueueClips[0] ?? slices[0] ?? null;
  }, [slices, activeClipId, visibleQueueClipIds]);

  const activeClipAudioUrl = useMemo(() => {
    if (!activeClip) {
      return null;
    }
    const revision = `${activeClip.active_variant_id ?? "no-variant"}:${activeClip.active_commit_id ?? "base"}`;
    return buildSliceAudioUrl(activeClip.id, revision);
  }, [activeClip]);

  function handleClipSelect(nextClipId: string) {
    startTransition(() => {
      setActiveClipId(nextClipId);
    });
  }

  function getNextClipId(currentClipId: string): string | null {
    if (visibleQueueClipIds.length === 0) {
      return null;
    }

    const currentIndex = visibleQueueClipIds.findIndex((clipId) => clipId === currentClipId);
    if (currentIndex < 0) {
      return visibleQueueClipIds[0] ?? null;
    }

    const nextClipId = visibleQueueClipIds[currentIndex + 1] ?? visibleQueueClipIds[0] ?? null;
    if (!nextClipId || nextClipId === currentClipId) {
      return null;
    }

    return nextClipId;
  }

  function replaceSlice(updatedSlice: Slice) {
    setSlices((current) => current.map((slice) => (slice.id === updatedSlice.id ? updatedSlice : slice)));
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
      setVisibleQueueClipIds(sorted.map((slice) => slice.id));
      setActiveClipId((current) =>
        sorted.some((slice) => slice.id === current) ? current : (sorted[0]?.id ?? null),
      );
      setWorkspaceNotice(
        `Cleanup removed ${result.deleted_slice_count} superseded slice(s), ${result.deleted_variant_count} variant row(s), and ${result.deleted_file_count} file(s).`,
      );
    } catch (error) {
      setWorkspaceNotice(getErrorMessage(error, "Project media cleanup failed."));
    } finally {
      setIsCleaningMedia(false);
    }
  }

  async function saveClipStatus(clipId: string, status: ReviewStatus): Promise<Slice> {
    const updatedSlice = await updateClipStatus(clipId, status);
    replaceSlice(updatedSlice);
    return updatedSlice;
  }

  async function saveClipTranscript(clipId: string, modifiedText: string): Promise<Slice> {
    const updatedSlice = await updateClipTranscript(clipId, modifiedText);
    replaceSlice(updatedSlice);
    return updatedSlice;
  }

  async function saveClipTags(
    clipId: string,
    tags: { name: string; color: string }[],
  ): Promise<Slice> {
    const updatedSlice = await updateClipTags(clipId, tags);
    replaceSlice(updatedSlice);
    return updatedSlice;
  }

  async function saveClipEdl(
    clipId: string,
    payload: {
      op: string;
      range?: { start_seconds: number; end_seconds: number } | null;
      duration_seconds?: number | null;
    },
  ): Promise<Slice> {
    const updatedSlice = await appendClipEdlOperation(clipId, payload);
    replaceSlice(updatedSlice);
    return updatedSlice;
  }

  async function undoClipMutation(clipId: string): Promise<Slice> {
    const updatedSlice = await undoClip(clipId);
    replaceSlice(updatedSlice);
    return updatedSlice;
  }

  async function redoClipMutation(clipId: string): Promise<Slice> {
    const updatedSlice = await redoClip(clipId);
    replaceSlice(updatedSlice);
    return updatedSlice;
  }

  async function splitClipMutation(clipId: string, splitAtSeconds: number): Promise<Slice[]> {
    const nextSlices = await splitClip(clipId, splitAtSeconds);
    setSlices(nextSlices);
    const sorted = sortClipsForQueue(nextSlices);
    setVisibleQueueClipIds(sorted.map((slice) => slice.id));
    setActiveClipId(sorted[0]?.id ?? null);
    return nextSlices;
  }

  async function mergeNextClipMutation(clipId: string): Promise<Slice[]> {
    const nextSlices = await mergeWithNextClip(clipId);
    setSlices(nextSlices);
    const sorted = sortClipsForQueue(nextSlices);
    setVisibleQueueClipIds(sorted.map((slice) => slice.id));
    setActiveClipId(sorted[0]?.id ?? null);
    return nextSlices;
  }

  async function runClipLabModelMutation(clipId: string, generatorModel: string): Promise<Slice> {
    const updatedSlice = await runClipLabModel(clipId, generatorModel);
    replaceSlice(updatedSlice);
    return updatedSlice;
  }

  async function setActiveVariantMutation(variantId: string) {
    if (!activeClip) {
      return;
    }
    try {
      const updatedSlice = await setActiveVariant(activeClip.id, variantId);
      replaceSlice(updatedSlice);
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
    datasetStatusCounts.counts.rejected > 0
      ? datasetStatusCounts.counts.accepted / datasetStatusCounts.counts.rejected
      : datasetStatusCounts.counts.accepted > 0
        ? datasetStatusCounts.counts.accepted
        : null;
  const predictedOutputSeconds =
    datasetStatusCounts.counts.accepted > 0 ? datasetStatusCounts.durations.accepted : null;
  const progressPercent =
    slices.length > 0
      ? ((datasetStatusCounts.counts.accepted + datasetStatusCounts.counts.rejected) / slices.length) * 100
      : null;
  const canUndo = Boolean(activeClip?.active_commit_id);
  const canRedo = activeClip ? Boolean(getRedoTarget(activeClip)) : false;

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
            clips={slices}
            activeClipId={activeClip?.id ?? null}
            onSelectClip={handleClipSelect}
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
            getNextClipId={getNextClipId}
            onSelectClip={handleClipSelect}
            onRetryLoad={() => void loadWorkspace(activeProject?.id)}
            onSaveTranscript={saveClipTranscript}
            onSaveTags={saveClipTags}
            onUpdateStatus={saveClipStatus}
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
              void saveClipStatus(activeClip.id, status);
            }}
            onVariantSelect={(variantId) => void setActiveVariantMutation(variantId)}
          />
        </div>
      </div>
    </ErrorBoundary>
  );
}
