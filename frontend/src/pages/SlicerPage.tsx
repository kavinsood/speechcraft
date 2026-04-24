import { useEffect, useMemo, useState } from "react";
import { ApiError, createProjectSlicerRun, deleteProjectSlicerRun, fetchProjectSlicerRuns } from "../api";
import JobActivityPanel, { type JobActivity } from "../components/JobActivityPanel";
import { usePipelineContext } from "../pipeline/PipelineContext";
import type { Project, SlicerRun, SlicerRunRequest } from "../types";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type SlicerPageProps = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
  onOpenQc: () => void;
};

type SlicerLoadStatus = "idle" | "loading" | "ready" | "error";

const defaultSlicerSettings: SlicerRunRequest = {
  target_clip_length: 7,
  max_clip_length: 15,
  segmentation_sensitivity: 0.5,
  preserve_locked_slices: true,
  replace_unlocked_slices: true,
  advanced_config_overrides: null,
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

function formatDuration(seconds?: number): string {
  if (!seconds || !Number.isFinite(seconds)) {
    return "0:00";
  }
  const rounded = Math.round(seconds);
  return `${Math.floor(rounded / 60)}:${String(rounded % 60).padStart(2, "0")}`;
}

function runToActivity(run: SlicerRun | null): JobActivity | null {
  if (!run) {
    return null;
  }

  const lines = run.jobs.flatMap((job, index) => {
    const outputLogs = Array.isArray(job.output_payload?.logs) ? job.output_payload.logs : [];
    const baseLine = {
      id: `${job.id}-status`,
      timestamp: String(index + 1).padStart(2, "0"),
      message:
        job.status === "failed"
          ? `${job.source_recording_id}: ${job.error_message ?? "Slicing failed"}`
          : `${job.source_recording_id}: ${job.status}`,
    };
    return [
      baseLine,
      ...outputLogs.map((line, logIndex) => ({
        id: `${job.id}-log-${logIndex}`,
        timestamp: `${index + 1}.${logIndex + 1}`,
        message: String(line),
      })),
    ];
  });

  return {
    id: run.id,
    name: `Slicer run ${run.id}`,
    type: "slicing",
    state: run.status === "pending" ? "running" : run.status,
    startedAt: run.started_at ?? run.created_at,
    completedAt: run.completed_at,
    progressLabel:
      run.status === "completed"
        ? "Slicer run completed"
        : run.status === "failed"
          ? "Slicer run failed"
          : run.status === "pending"
            ? "Queued for worker"
            : "Slicing recordings",
    logs: lines,
  };
}

export default function SlicerPage({
  activeProject,
  projectLoadStatus,
  projectLoadError,
  onRetryProjects,
  onOpenQc,
}: SlicerPageProps) {
  const { selectedSlicerRunId, selectSlicerRun } = usePipelineContext();
  const [loadStatus, setLoadStatus] = useState<SlicerLoadStatus>("idle");
  const [loadError, setLoadError] = useState<string | null>(null);
  const [runs, setRuns] = useState<SlicerRun[]>([]);
  const [settings, setSettings] = useState<SlicerRunRequest>(defaultSlicerSettings);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [advancedJson, setAdvancedJson] = useState("{}");
  const [launchError, setLaunchError] = useState<string | null>(null);
  const [deleteError, setDeleteError] = useState<string | null>(null);

  async function loadRuns(projectId: string) {
    setLoadStatus("loading");
    setLoadError(null);
    try {
      const nextRuns = await fetchProjectSlicerRuns(projectId);
      setRuns(nextRuns);
      setLoadStatus("ready");
      if (!selectedSlicerRunId && nextRuns[0]) {
        selectSlicerRun(nextRuns[0].id);
      }
    } catch (error) {
      setRuns([]);
      setLoadStatus("error");
      setLoadError(getErrorMessage(error, "Slicer runs could not be loaded."));
    }
  }

  useEffect(() => {
    setRuns([]);
    setLaunchError(null);
    setDeleteError(null);
    if (!activeProject) {
      setLoadStatus("idle");
      return;
    }
    void loadRuns(activeProject.id);
  }, [activeProject?.id]);

  const selectedRun = useMemo(
    () => runs.find((run) => run.id === selectedSlicerRunId) ?? null,
    [runs, selectedSlicerRunId],
  );
  const activeRun = useMemo(
    () => runs.find((run) => run.status === "pending" || run.status === "running") ?? null,
    [runs],
  );

  useEffect(() => {
    if (!activeProject || !activeRun) {
      return;
    }
    let cancelled = false;
    let timeoutId: number | null = null;

    const poll = async () => {
      await loadRuns(activeProject.id);
      if (!cancelled) {
        timeoutId = window.setTimeout(() => {
          void poll();
        }, 2500);
      }
    };

    timeoutId = window.setTimeout(() => {
      void poll();
    }, 2500);

    return () => {
      cancelled = true;
      if (timeoutId !== null) {
        window.clearTimeout(timeoutId);
      }
    };
  }, [activeProject?.id, activeRun?.id]);

  async function handleLaunchRun() {
    if (!activeProject) {
      return;
    }
    setLaunchError(null);
    let advanced_config_overrides: Record<string, unknown> | null = null;
    if (showAdvanced && advancedJson.trim() && advancedJson.trim() !== "{}") {
      try {
        const parsed = JSON.parse(advancedJson) as unknown;
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
          throw new Error("Advanced overrides must be a JSON object.");
        }
        advanced_config_overrides = parsed as Record<string, unknown>;
      } catch (error) {
        setLaunchError(getErrorMessage(error, "Advanced overrides must be valid JSON."));
        return;
      }
    }

    try {
      const run = await createProjectSlicerRun(activeProject.id, {
        ...settings,
        advanced_config_overrides,
      });
      setRuns((current) => [run, ...current.filter((entry) => entry.id !== run.id)]);
      selectSlicerRun(run.id);
    } catch (error) {
      setLaunchError(getErrorMessage(error, "Slicer run could not be launched."));
    }
  }

  async function handleDeleteSelectedRun() {
    if (!activeProject || !selectedRun) {
      return;
    }
    setDeleteError(null);
    if (selectedRun.status === "pending" || selectedRun.status === "running") {
      setDeleteError("Slicer runs can only be deleted after they complete or fail.");
      return;
    }
    const confirmed = window.confirm(
      `Delete slicer run ${selectedRun.id}? This removes generated slices, QC runs, and unreferenced media files for this run.`,
    );
    if (!confirmed) {
      return;
    }

    try {
      await deleteProjectSlicerRun(activeProject.id, selectedRun.id);
      const remainingRuns = runs.filter((run) => run.id !== selectedRun.id);
      setRuns(remainingRuns);
      selectSlicerRun(remainingRuns[0]?.id ?? null);
    } catch (error) {
      setDeleteError(getErrorMessage(error, "Slicer run could not be deleted."));
    }
  }

  if (projectLoadStatus === "error") {
    return (
      <WorkspaceStatePanel
        title="Projects unavailable"
        message={projectLoadError ?? "The project list could not be loaded."}
        actionLabel="Retry project load"
        onAction={onRetryProjects}
      />
    );
  }

  if (projectLoadStatus === "loading") {
    return <WorkspaceStatePanel title="Loading projects" message="Fetching project context." />;
  }

  if (!activeProject) {
    return (
      <WorkspaceStatePanel
        title="No project selected"
        message="Select a project before creating or choosing slicer runs."
      />
    );
  }

  if (loadStatus === "loading" && runs.length === 0) {
    return <WorkspaceStatePanel title="Loading slicer runs" message="Reading run history." />;
  }

  if (loadStatus === "error") {
    return (
      <WorkspaceStatePanel
        title="Slicer runs unavailable"
        message={loadError ?? "Slicer runs could not be loaded."}
        actionLabel="Retry slicer runs"
        onAction={() => void loadRuns(activeProject.id)}
      />
    );
  }

  const canLaunch = Boolean(activeProject.active_prepared_output_group_id) && !activeRun;

  return (
    <section className="step-page pipeline-page slicer-page">
      <div className="stage-layout">
        <aside className="stage-sidebar panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Run selector</p>
              <h3>Slicer runs</h3>
            </div>
          </div>
          <ul className="stage-list">
            {runs.length > 0 ? (
              runs.map((run) => (
                <li key={run.id}>
                  <button
                    className="pipeline-list-button"
                    type="button"
                    onClick={() => selectSlicerRun(run.id)}
                    aria-pressed={selectedRun?.id === run.id}
                  >
                    <strong>{run.id}</strong>
                    <span>
                      {run.status}
                      {run.is_stale ? " · stale" : ""} · {run.summary.slices_created ?? 0} slice(s)
                    </span>
                  </button>
                </li>
              ))
            ) : (
              <li>
                <strong>No slicer runs</strong>
                <span>Launch a run after preparation completes.</span>
              </li>
            )}
          </ul>
          <button
            className="ghost-button pipeline-full-button"
            type="button"
            onClick={handleDeleteSelectedRun}
            disabled={!selectedRun || selectedRun.status === "pending" || selectedRun.status === "running"}
          >
            Delete selected run
          </button>
          {deleteError ? <p className="shell-notice shell-notice-error">{deleteError}</p> : null}
        </aside>

        <main className="stage-main">
          <section className="panel pipeline-hero">
            <p className="eyebrow">Slicer scope</p>
            <h3>{selectedRun ? selectedRun.id : "No slicer run selected"}</h3>
            <p>
              Slicer creates distinct candidate-slice runs over the active prepared output group.
              It does not own QC buckets or manual Lab review.
            </p>
            <button
              className="primary-button"
              type="button"
              onClick={handleLaunchRun}
              disabled={!canLaunch}
            >
              Launch slicer run
            </button>
            {launchError ? <p className="shell-notice shell-notice-error">{launchError}</p> : null}
          </section>

          <section className="panel overview-prep-panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Top-level controls</p>
                <h3>Segmentation settings</h3>
              </div>
            </div>
            <div className="overview-control-grid">
              <label>
                <span>Target clip length</span>
                <input
                  className="search-input"
                  type="number"
                  min="1"
                  max="30"
                  value={settings.target_clip_length}
                  onChange={(event) =>
                    setSettings((current) => ({
                      ...current,
                      target_clip_length: Number(event.target.value),
                    }))
                  }
                />
              </label>
              <label>
                <span>Maximum clip length</span>
                <input
                  className="search-input"
                  type="number"
                  min="1"
                  max="60"
                  value={settings.max_clip_length}
                  onChange={(event) =>
                    setSettings((current) => ({
                      ...current,
                      max_clip_length: Number(event.target.value),
                    }))
                  }
                />
              </label>
              <label>
                <span>Segmentation sensitivity</span>
                <input
                  type="range"
                  min="0"
                  max="1"
                  step="0.05"
                  value={settings.segmentation_sensitivity}
                  onChange={(event) =>
                    setSettings((current) => ({
                      ...current,
                      segmentation_sensitivity: Number(event.target.value),
                    }))
                  }
                />
              </label>
            </div>
            <button className="ghost-button" type="button" onClick={() => setShowAdvanced((value) => !value)}>
              {showAdvanced ? "Hide advanced" : "Show advanced"}
            </button>
            {showAdvanced ? (
              <textarea
                className="transcript-editor slicer-advanced-json"
                value={advancedJson}
                onChange={(event) => setAdvancedJson(event.target.value)}
                rows={5}
              />
            ) : null}
          </section>

          <section className="pipeline-card-grid">
            <article className="panel pipeline-card">
              <p className="eyebrow">Run summary</p>
              <h3>{selectedRun?.status ?? "No run"}</h3>
              <p>Slices: {selectedRun?.summary.slices_created ?? 0}</p>
              <p>Total duration: {formatDuration(selectedRun?.summary.total_sliced_duration)}</p>
              <p>Average length: {selectedRun?.summary.average_slice_length?.toFixed(2) ?? "0.00"}s</p>
            </article>
            <article className="panel pipeline-card">
              <p className="eyebrow">Range and preservation</p>
              <h3>
                {selectedRun
                  ? `${selectedRun.summary.minimum_slice_length ?? 0}s - ${selectedRun.summary.maximum_slice_length ?? 0}s`
                  : "No data"}
              </h3>
              <p>Preserved locked: {selectedRun?.summary.preserved_locked_slice_count ?? 0}</p>
              <p>Dropped overlaps: {selectedRun?.summary.dropped_overlap_count ?? 0}</p>
              <p>QC data: {selectedRun?.summary.downstream_qc_data_available ? "available" : "not available"}</p>
            </article>
          </section>

          <JobActivityPanel title="Slicing activity" job={runToActivity(activeRun ?? selectedRun)} />
        </main>

        <aside className="stage-sidebar panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Next stage</p>
              <h3>QC handoff</h3>
            </div>
          </div>
          <ul className="stage-list">
            <li>
              <strong>Prepared input</strong>
              <span>{activeProject.active_prepared_output_group_id ?? "No active prepared output"}</span>
            </li>
            <li>
              <strong>Selected run</strong>
              <span>{selectedRun?.id ?? "Choose a completed run before opening QC"}</span>
            </li>
            {selectedRun?.warnings.map((warning) => (
              <li key={warning}>
                <strong>Warning</strong>
                <span>{warning}</span>
              </li>
            ))}
            {selectedRun?.is_stale ? (
              <li>
                <strong>Stale run</strong>
                <span>{selectedRun.stale_reason ?? "ASR or alignment changed after this run."}</span>
              </li>
            ) : null}
          </ul>
          <button
            className="primary-button pipeline-full-button"
            type="button"
            onClick={onOpenQc}
            disabled={!selectedRun || selectedRun.status !== "completed" || selectedRun.is_stale}
          >
            Open QC
          </button>
        </aside>
      </div>
    </section>
  );
}
