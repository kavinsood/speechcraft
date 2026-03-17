import { startTransition, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import {
  appendClipEdlOperation,
  ApiError,
  buildClipAudioUrl,
  commitClip,
  fetchClipCommits,
  fetchProjectDetail,
  fetchProjectExports,
  mergeWithNextClip,
  redoClip,
  runProjectExport,
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
  recalculateProjectDetail,
  sortClipsForQueue,
  statusLabels,
} from "../workspace/workspace-helpers";
import type {
  Clip,
  ClipCommit,
  ClipHistoryResult,
  ClipMutationResult,
  ExportRun,
  Project,
  ProjectDetail,
  ReviewStatus,
} from "../types";

type HistoryFlags = {
  can_undo: boolean;
  can_redo: boolean;
};

type WorkspaceStatus = "loading" | "error" | "ready";

type LabelPageProps = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
  onHeaderActionsChange: (actions: ReactNode) => void;
};

function replaceClipInProject(detail: ProjectDetail, updatedClip: Clip): ProjectDetail {
  return recalculateProjectDetail({
    ...detail,
    clips: detail.clips.map((clip) => (clip.id === updatedClip.id ? updatedClip : clip)),
  });
}

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
  const [projectDetail, setProjectDetail] = useState<ProjectDetail | null>(null);
  const [activeClipId, setActiveClipId] = useState<string | null>(null);
  const [visibleQueueClipIds, setVisibleQueueClipIds] = useState<string[]>([]);
  const [exportRuns, setExportRuns] = useState<ExportRun[]>([]);
  const [isRunningExport, setIsRunningExport] = useState(false);
  const [clipCommits, setClipCommits] = useState<Record<string, ClipCommit[]>>({});
  const [historyByClip, setHistoryByClip] = useState<Record<string, HistoryFlags>>({});
  const latestWorkspaceRequestRef = useRef(0);

  async function loadWorkspace(projectId: string | null | undefined) {
    const requestId = latestWorkspaceRequestRef.current + 1;
    latestWorkspaceRequestRef.current = requestId;
    setWorkspaceStatus("loading");
    setWorkspaceError(null);
    setWorkspaceEmptyMessage(null);
    setWorkspaceNotice(null);

    if (!projectId) {
      setProjectDetail(null);
      setExportRuns([]);
      setActiveClipId(null);
      setVisibleQueueClipIds([]);
      setWorkspaceStatus("ready");
      setWorkspaceEmptyMessage("Select a project to open the review workstation.");
      return;
    }

    try {
      const [detail, exports] = await Promise.all([
        fetchProjectDetail(projectId),
        fetchProjectExports(projectId),
      ]);

      if (latestWorkspaceRequestRef.current !== requestId) {
        return;
      }

      setProjectDetail(detail);
      setExportRuns(exports);
      setVisibleQueueClipIds(sortClipsForQueue(detail.clips).map((clip) => clip.id));
      setActiveClipId((current) =>
        detail.clips.some((clip) => clip.id === current) ? current : (detail.clips[0]?.id ?? null),
      );
      setWorkspaceStatus("ready");
      setWorkspaceEmptyMessage(null);
    } catch (error) {
      if (latestWorkspaceRequestRef.current !== requestId) {
        return;
      }

      setProjectDetail(null);
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
        (projectDetail?.clips ?? [])
          .flatMap((clip) => clip.tags.map((tag) => tag.name.toLowerCase()))
          .sort(),
      ),
    );
  }, [projectDetail?.clips]);

  const activeClip = useMemo(() => {
    const allClips = projectDetail?.clips ?? [];
    const visibleQueueClips = visibleQueueClipIds
      .map((clipId) => allClips.find((clip) => clip.id === clipId) ?? null)
      .filter((clip): clip is Clip => clip !== null);

    return (
      allClips.find((clip) => clip.id === activeClipId) ??
      visibleQueueClips[0] ??
      allClips[0] ??
      null
    );
  }, [projectDetail?.clips, activeClipId, visibleQueueClipIds]);

  useEffect(() => {
    if (!activeClip) {
      return;
    }

    if (clipCommits[activeClip.id]) {
      return;
    }

    let cancelled = false;

    void (async () => {
      try {
        const commits = await fetchClipCommits(activeClip.id);
        if (cancelled) {
          return;
        }

        setClipCommits((current) => ({
          ...current,
          [activeClip.id]: commits,
        }));
      } catch (error) {
        if (cancelled) {
          return;
        }

        const message = getErrorMessage(error, "Commit history failed to load for this clip.");
        console.error(message);
        setWorkspaceNotice(message);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [activeClip?.id, clipCommits]);

  function updateCurrentClip(updatedClip: Clip) {
    setProjectDetail((current) => {
      if (!current) {
        return current;
      }

      return replaceClipInProject(current, updatedClip);
    });
    setHistoryByClip((current) => ({
      ...current,
      [updatedClip.id]: { can_undo: true, can_redo: false },
    }));
  }

  function applyHistoryResult(result: ClipHistoryResult) {
    setProjectDetail((current) => {
      if (!current) {
        return current;
      }

      return replaceClipInProject(current, result.clip);
    });
    setHistoryByClip((current) => ({
      ...current,
      [result.clip.id]: {
        can_undo: result.can_undo,
        can_redo: result.can_redo,
      },
    }));
  }

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

  async function handleRunExport() {
    if (!projectDetail) {
      return;
    }

    setIsRunningExport(true);
    try {
      const result = await runProjectExport(projectDetail.project.id);
      const [detail, exports] = await Promise.all([
        fetchProjectDetail(projectDetail.project.id),
        fetchProjectExports(projectDetail.project.id),
      ]);
      setProjectDetail(detail);
      setExportRuns(exports);
      setWorkspaceNotice(
        `Export completed: ${result.accepted_clip_count} accepted clip(s) rendered.`,
      );
    } catch (error) {
      setWorkspaceNotice(getErrorMessage(error, "Export failed. See backend logs for details."));
    } finally {
      setIsRunningExport(false);
    }
  }

  async function saveClipStatus(clipId: string, reviewStatus: ReviewStatus): Promise<Clip> {
    const updatedClip = await updateClipStatus(clipId, reviewStatus);
    updateCurrentClip(updatedClip);
    return updatedClip;
  }

  async function saveClipTranscript(clipId: string, textCurrent: string): Promise<Clip> {
    const updatedClip = await updateClipTranscript(clipId, textCurrent);
    updateCurrentClip(updatedClip);
    return updatedClip;
  }

  async function saveClipTags(
    clipId: string,
    tags: { name: string; color: string }[],
  ): Promise<Clip> {
    const updatedClip = await updateClipTags(clipId, tags);
    updateCurrentClip(updatedClip);
    return updatedClip;
  }

  async function applyClipEdlOperation(
    clipId: string,
    payload: {
      op: string;
      range?: { start_seconds: number; end_seconds: number } | null;
      duration_seconds?: number | null;
    },
  ): Promise<Clip> {
    const updatedClip = await appendClipEdlOperation(clipId, payload);
    updateCurrentClip(updatedClip);
    return updatedClip;
  }

  async function createCommitSnapshot(workingClip: Clip, message: string): Promise<ClipCommit> {
    const createdCommit = await commitClip(workingClip.id, message);
    const committedClip: Clip = {
      ...workingClip,
      edit_state: "committed",
      updated_at: new Date().toISOString(),
    };
    setProjectDetail((current) =>
      current ? replaceClipInProject(current, committedClip) : current,
    );
    setClipCommits((current) => ({
      ...current,
      [workingClip.id]: [...(current[workingClip.id] ?? []), createdCommit],
    }));
    setHistoryByClip((current) => ({
      ...current,
      [workingClip.id]: { can_undo: true, can_redo: false },
    }));
    return createdCommit;
  }

  async function undoClipMutation(clipId: string): Promise<ClipHistoryResult> {
    const result = await undoClip(clipId);
    applyHistoryResult(result);
    return result;
  }

  async function redoClipMutation(clipId: string): Promise<ClipHistoryResult> {
    const result = await redoClip(clipId);
    applyHistoryResult(result);
    return result;
  }

  async function splitClipMutation(
    clipId: string,
    splitAtSeconds: number,
  ): Promise<ClipMutationResult> {
    const result = await splitClip(clipId, splitAtSeconds);
    setProjectDetail(result.project_detail);
    setActiveClipId(result.created_clip_ids[0] ?? null);
    return result;
  }

  async function mergeNextClipMutation(clipId: string): Promise<ClipMutationResult> {
    const result = await mergeWithNextClip(clipId);
    setProjectDetail(result.project_detail);
    setActiveClipId(result.created_clip_ids[0] ?? null);
    return result;
  }

  async function handleStatusChange(reviewStatus: ReviewStatus) {
    if (!activeClip) {
      return;
    }

    try {
      if (reviewStatus === "accepted") {
        const acceptedClip = await saveClipStatus(activeClip.id, "accepted");
        await createCommitSnapshot(acceptedClip, "Accepted clip snapshot");
        setWorkspaceNotice("Marked clip as accepted and committed the snapshot.");
        return;
      }

      if (reviewStatus === "rejected") {
        const rejectedClip = await saveClipStatus(activeClip.id, "rejected");
        await createCommitSnapshot(rejectedClip, "Rejected clip snapshot");
        setWorkspaceNotice("Marked clip as rejected and committed the snapshot.");
        return;
      }

      await saveClipStatus(activeClip.id, reviewStatus);
      setWorkspaceNotice(`Marked clip as ${statusLabels[reviewStatus].toLowerCase()}.`);
    } catch (error) {
      setWorkspaceNotice(getErrorMessage(error, "Status update failed. Check the backend."));
    }
  }

  const activeCommits = activeClip ? clipCommits[activeClip.id] ?? [] : [];
  const activeHistory = activeClip
    ? historyByClip[activeClip.id] ?? { can_undo: false, can_redo: false }
    : { can_undo: false, can_redo: false };
  const datasetStatusCounts = useMemo(() => {
    const counts: Record<ReviewStatus, number> = {
      candidate: 0,
      needs_attention: 0,
      in_review: 0,
      accepted: 0,
      rejected: 0,
    };
    const durations: Record<ReviewStatus, number> = {
      candidate: 0,
      needs_attention: 0,
      in_review: 0,
      accepted: 0,
      rejected: 0,
    };
    for (const clip of projectDetail?.clips ?? []) {
      counts[clip.review_status] += 1;
      durations[clip.review_status] += clip.duration_seconds;
    }
    return { counts, durations };
  }, [projectDetail?.clips]);
  const acceptedRejectedRatio =
    datasetStatusCounts.durations.rejected > 0
      ? datasetStatusCounts.durations.accepted / datasetStatusCounts.durations.rejected
      : null;
  const totalDurationSeconds = projectDetail?.stats.total_duration_seconds ?? 0;
  const resolvedDurationSeconds =
    datasetStatusCounts.durations.accepted + datasetStatusCounts.durations.rejected;
  const smoothingSeconds = 10;
  const smoothedAcceptRate =
    resolvedDurationSeconds >= 0
      ? (datasetStatusCounts.durations.accepted + smoothingSeconds) /
        (resolvedDurationSeconds + smoothingSeconds * 2)
      : null;
  const predictedOutputSeconds =
    smoothedAcceptRate !== null
      ? Math.min(totalDurationSeconds, Math.max(0, totalDurationSeconds * smoothedAcceptRate))
      : null;
  const progressPercent =
    totalDurationSeconds > 0
      ? Math.min(100, Math.max(0, (resolvedDurationSeconds / totalDurationSeconds) * 100))
      : null;
  const workspacePhase =
    workspaceStatus === "loading"
      ? "loading"
      : workspaceStatus === "error"
        ? "error"
        : projectDetail
          ? "ready"
          : "empty";
  const workspaceStatusLabel =
    workspacePhase === "ready"
      ? `Export: ${(projectDetail?.project.export_status ?? "not_exported").replace(/_/g, " ")}`
      : workspacePhase === "error"
        ? "Workspace load failed"
        : workspacePhase === "empty"
          ? "No project loaded"
          : "Loading workspace";
  const workspaceFallback = (
    <main className="workspace-grid">
      <section className="panel workspace-pane">
        <WorkspaceStatePanel
          title="Workspace pane crashed"
          message="A render error interrupted this view. Reload the workspace to recover."
          actionLabel="Reload workspace"
          onAction={() => void loadWorkspace(activeProject?.id)}
        />
      </section>
      <section className="panel workspace-pane">
        <WorkspaceStatePanel
          title="Editor unavailable"
          message="The active view threw while rendering. The backend state is untouched."
        />
      </section>
      <section className="panel workspace-pane">
        <WorkspaceStatePanel
          title="Inspector unavailable"
          message="Reload after checking the console for the underlying UI exception."
        />
      </section>
    </main>
  );

  useEffect(() => {
    onHeaderActionsChange(
      <>
        <span className="status-pill">{workspaceStatusLabel}</span>
        {workspacePhase === "error" ? (
          <button className="ghost-button" type="button" onClick={onRetryProjects}>
            Retry projects
          </button>
        ) : null}
        <button
          className="primary-button"
          type="button"
          onClick={handleRunExport}
          disabled={workspacePhase !== "ready" || isRunningExport}
        >
          {isRunningExport ? "Rendering..." : "Run Export"}
        </button>
      </>,
    );

    return () => {
      onHeaderActionsChange(null);
    };
  }, [
    onHeaderActionsChange,
    workspaceStatusLabel,
    workspacePhase,
    isRunningExport,
    onRetryProjects,
    projectDetail?.project.id,
  ]);

  return (
    <section className="step-page label-page">
      {workspaceNotice ? <p className="editor-notice">{workspaceNotice}</p> : null}

      <ErrorBoundary
        resetKey={`${workspacePhase}:${projectDetail?.project.id ?? "none"}`}
        fallback={workspaceFallback}
      >
        <main className="workspace-grid workspace-grid-nested">
          <ClipQueuePane
            workspacePhase={workspacePhase}
            workspaceError={workspaceError}
            workspaceEmptyMessage={workspaceEmptyMessage}
            clips={projectDetail?.clips ?? []}
            activeClipId={activeClip?.id ?? null}
            onSelectClip={handleClipSelect}
            onRetryLoad={() => void loadWorkspace(activeProject?.id)}
            onVisibleClipIdsChange={setVisibleQueueClipIds}
          />

          <EditorPane
            workspacePhase={workspacePhase}
            workspaceError={workspaceError}
            workspaceEmptyMessage={workspaceEmptyMessage}
            activeClip={activeClip}
            activeClipAudioUrl={activeClip ? buildClipAudioUrl(activeClip.id) : null}
            activeHistory={activeHistory}
            allClipTagNames={allClipTagNames}
            getNextClipId={getNextClipId}
            onSelectClip={handleClipSelect}
            onRetryLoad={() => void loadWorkspace(activeProject?.id)}
            onSaveTranscript={saveClipTranscript}
            onSaveTags={saveClipTags}
            onUpdateStatus={saveClipStatus}
            onCommitSnapshot={createCommitSnapshot}
            onAppendEdlOperation={applyClipEdlOperation}
            onUndo={undoClipMutation}
            onRedo={redoClipMutation}
            onSplitClip={splitClipMutation}
            onMergeClip={mergeNextClipMutation}
          />

          <InspectorPane
            workspacePhase={workspacePhase}
            workspaceError={workspaceError}
            activeClip={activeClip}
            projectDetail={projectDetail}
            datasetStatusCounts={datasetStatusCounts}
            acceptedRejectedRatio={acceptedRejectedRatio}
            predictedOutputSeconds={predictedOutputSeconds}
            progressPercent={progressPercent}
            activeCommits={activeCommits}
            exportRuns={exportRuns}
            onRetryLoad={() => void loadWorkspace(activeProject?.id)}
            onStatusChange={(status) => void handleStatusChange(status)}
          />
        </main>
      </ErrorBoundary>
    </section>
  );
}
