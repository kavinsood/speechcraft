import { startTransition, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import {
  ApiError,
  appendClipEdlOperation,
  buildSliceAudioUrl,
  cleanupProjectMedia,
  fetchProjectReferenceAssets,
  fetchProjectExports,
  fetchSliceDetail,
  fetchProjectSlices,
  mergeWithNextClip,
  redoClip,
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
import {
  getSliceAudioRevisionKey,
  getSliceDuration,
  sortClipsForQueue,
} from "../workspace/workspace-helpers";
import type {
  ExportRun,
  Project,
  ReferenceAssetSummary,
  ReviewStatus,
  Slice,
  SliceSummary,
} from "../types";

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
  projectLoadStatus,
  projectLoadError,
  onRetryProjects,
  onHeaderActionsChange,
}: LabelPageProps) {
  const [workspaceStatus, setWorkspaceStatus] = useState<WorkspaceStatus>("loading");
  const [workspaceError, setWorkspaceError] = useState<string | null>(null);
  const [workspaceEmptyMessage, setWorkspaceEmptyMessage] = useState<string | null>(null);
  const [workspaceNotice, setWorkspaceNotice] = useState<string | null>(null);
  const [slices, setSlices] = useState<SliceSummary[]>([]);
  const [activeClip, setActiveClip] = useState<Slice | null>(null);
  const [activeClipId, setActiveClipId] = useState<string | null>(null);
  const [visibleQueueClipIds, setVisibleQueueClipIds] = useState<string[]>([]);
  const [exportRuns, setExportRuns] = useState<ExportRun[]>([]);
  const [referenceAssets, setReferenceAssets] = useState<ReferenceAssetSummary[]>([]);
  const [isRunningExport, setIsRunningExport] = useState(false);
  const [isCleaningMedia, setIsCleaningMedia] = useState(false);
  const [isSavingReference, setIsSavingReference] = useState(false);
  const latestWorkspaceRequestRef = useRef(0);
  const latestDetailRequestRef = useRef(0);
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
      setReferenceAssets([]);
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
      setActiveClip(null);
      setExportRuns(nextExports);
      setReferenceAssets(nextReferenceAssets);
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
      setActiveClip(null);
      setExportRuns([]);
      setReferenceAssets([]);
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

  const sliceMap = useMemo(() => new Map(slices.map((slice) => [slice.id, slice])), [slices]);

  const activeClipSummary = useMemo(() => {
    const visibleQueueClips = visibleQueueClipIds
      .map((clipId) => sliceMap.get(clipId) ?? null)
      .filter((slice): slice is SliceSummary => slice !== null);

    return sliceMap.get(activeClipId ?? "") ?? visibleQueueClips[0] ?? slices[0] ?? null;
  }, [sliceMap, slices, activeClipId, visibleQueueClipIds]);

  const activeClipRevisionKey = activeClipSummary
    ? `${activeClipSummary.active_variant_id ?? "no-variant"}:${activeClipSummary.active_commit_id ?? "base"}`
    : null;

  useEffect(() => {
    const requestId = latestDetailRequestRef.current + 1;
    latestDetailRequestRef.current = requestId;

    if (!activeClipSummary) {
      setActiveClip(null);
      return;
    }

    void (async () => {
      try {
        const detail = await fetchSliceDetail(activeClipSummary.id);
        if (latestDetailRequestRef.current !== requestId) {
          return;
        }
        setActiveClip(detail);
      } catch (error) {
        if (latestDetailRequestRef.current !== requestId) {
          return;
        }
        setActiveClip(null);
        setWorkspaceNotice(getErrorMessage(error, "The active slice failed to load."));
      }
    })();
  }, [activeClipSummary?.id, activeClipRevisionKey]);

  const activeClipAudioUrl = useMemo(() => {
    if (!activeClip) {
      return null;
    }
    const revision = getSliceAudioRevisionKey(activeClip);
    return buildSliceAudioUrl(activeClip.id, revision);
  }, [activeClip]);

  function handleClipSelect(nextClipId: string) {
    setActiveClip(null);
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
    setSlices((current) =>
      current.map((slice) => (slice.id === updatedSlice.id ? summarizeSlice(updatedSlice) : slice)),
    );
    setActiveClip((current) => (current?.id === updatedSlice.id ? updatedSlice : current));
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

  async function saveFullSlice(
    clipId: string,
    payload: {
      modified_text?: string | null;
      tags?: { name: string; color: string }[] | null;
      status?: ReviewStatus | null;
      message?: string | null;
      is_milestone?: boolean;
    },
  ): Promise<Slice> {
    const updatedSlice = await saveClipState(clipId, payload);
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

  async function splitClipMutation(clipId: string, splitAtSeconds: number): Promise<SliceSummary[]> {
    const existingIds = new Set(slices.map((slice) => slice.id));
    const nextSlices = await splitClip(clipId, splitAtSeconds);
    setSlices(nextSlices);
    setActiveClip(null);
    const sorted = sortClipsForQueue(nextSlices);
    setVisibleQueueClipIds(sorted.map((slice) => slice.id));
    const nextActiveClip = sorted.find((slice) => !existingIds.has(slice.id)) ?? sorted[0] ?? null;
    setActiveClipId(nextActiveClip?.id ?? null);
    return nextSlices;
  }

  async function mergeNextClipMutation(clipId: string): Promise<SliceSummary[]> {
    const existingIds = new Set(slices.map((slice) => slice.id));
    const nextSlices = await mergeWithNextClip(clipId);
    setSlices(nextSlices);
    setActiveClip(null);
    const sorted = sortClipsForQueue(nextSlices);
    setVisibleQueueClipIds(sorted.map((slice) => slice.id));
    const nextActiveClip = sorted.find((slice) => !existingIds.has(slice.id)) ?? sorted[0] ?? null;
    setActiveClipId(nextActiveClip?.id ?? null);
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

  async function handleSaveAsReference(options?: {
    name?: string | null;
    mood_label?: string | null;
  }) {
    if (!activeClip) {
      return;
    }
    if (!activeClip.active_variant_id) {
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
  const existingReferenceForCurrentState = useMemo(() => {
    if (!activeClip?.active_commit_id) {
      return null;
    }
    return (
      referenceAssets.find(
        (asset) =>
          asset.source_slice_id === activeClip.id
          && asset.source_edit_commit_id === activeClip.active_commit_id,
      ) ?? null
    );
  }, [activeClip?.active_commit_id, activeClip?.id, referenceAssets]);

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
            onSaveSlice={saveFullSlice}
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
              void saveFullSlice(activeClip.id, {
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
