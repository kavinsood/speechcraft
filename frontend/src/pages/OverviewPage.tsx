import { useEffect, useMemo, useState } from "react";
import {
  ApiError,
  fetchProcessingJob,
  fetchProjectPreparationJobs,
  fetchProjectRecordings,
  runProjectPreparation,
} from "../api";
import JobActivityPanel, { type JobActivity } from "../components/JobActivityPanel";
import type {
  PreparationSettings,
  ProcessingJob,
  Project,
  SourceRecordingQueue,
} from "../types";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type OverviewPageProps = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
};

type RecordingLoadStatus = "idle" | "loading" | "ready" | "error";

const defaultPrepSettings: PreparationSettings = {
  target_sample_rate: 24000,
  channel_mode: "mono",
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

function formatDuration(totalSeconds: number): string {
  if (!Number.isFinite(totalSeconds) || totalSeconds <= 0) {
    return "0:00";
  }

  const rounded = Math.round(totalSeconds);
  const hours = Math.floor(rounded / 3600);
  const minutes = Math.floor((rounded % 3600) / 60);
  const seconds = rounded % 60;

  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }

  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

function formatSet(values: number[]): string {
  if (values.length === 0) {
    return "None";
  }

  return values.join(", ");
}

function stableSettingsKey(settings: PreparationSettings): string {
  return JSON.stringify({
    target_sample_rate: settings.target_sample_rate ?? null,
    channel_mode: settings.channel_mode,
  });
}

function parsePreparationRecipe(recording: SourceRecordingQueue): {
  outputGroupId?: string;
  settings?: PreparationSettings;
} | null {
  if (!recording.processing_recipe) {
    return null;
  }

  try {
    const parsed = JSON.parse(recording.processing_recipe) as {
      type?: string;
      output_group_id?: string;
      settings?: PreparationSettings;
    };
    if (parsed.type !== "overview_preparation") {
      return null;
    }
    return {
      outputGroupId: parsed.output_group_id,
      settings: parsed.settings,
    };
  } catch {
    return null;
  }
}

function jobToActivity(job: ProcessingJob, fallbackName: string): JobActivity {
  const logs = Array.isArray(job.output_payload?.logs)
    ? (job.output_payload.logs as unknown[])
        .map((line, index) => ({
          id: `${job.id}-log-${index}`,
          timestamp: String(index + 1).padStart(2, "0"),
          message: String(line),
        }))
    : [];

  return {
    id: job.id,
    name: fallbackName,
    type: "preparation",
    state: job.status === "pending" ? "running" : job.status,
    startedAt: job.started_at ?? job.created_at,
    completedAt: job.completed_at,
    progressLabel:
      job.status === "completed"
        ? "Prepared output created"
        : job.status === "failed"
          ? job.error_message ?? "Preparation failed"
          : job.status === "pending"
            ? "Queued for worker"
            : "Preparation running",
    logs:
      logs.length > 0
        ? logs
        : [
            {
              id: `${job.id}-status`,
              timestamp: "01",
              message:
                job.status === "failed"
                  ? job.error_message ?? "Preparation failed."
                  : job.status === "pending"
                    ? "Preparation job is queued. Start the backend worker to process it."
                    : `Preparation job ${job.status}.`,
            },
          ],
  };
}

export default function OverviewPage({
  activeProject,
  projectLoadStatus,
  projectLoadError,
  onRetryProjects,
}: OverviewPageProps) {
  const [recordingStatus, setRecordingStatus] = useState<RecordingLoadStatus>("idle");
  const [recordingError, setRecordingError] = useState<string | null>(null);
  const [recordings, setRecordings] = useState<SourceRecordingQueue[]>([]);
  const [prepJobs, setPrepJobs] = useState<ProcessingJob[]>([]);
  const [prepSettings, setPrepSettings] = useState<PreparationSettings>(defaultPrepSettings);

  async function loadRecordings(projectId: string) {
    setRecordingStatus((current) => (current === "ready" ? current : "loading"));
    setRecordingError(null);

    try {
      const nextRecordings = await fetchProjectRecordings(projectId);
      setRecordings(nextRecordings);
      setRecordingStatus("ready");
    } catch (error) {
      setRecordings([]);
      setRecordingStatus("error");
      setRecordingError(getErrorMessage(error, "Source recordings could not be loaded."));
    }
  }

  async function loadPreparationJobs(projectId: string) {
    const jobs = await fetchProjectPreparationJobs(projectId);
    setPrepJobs(jobs);
  }

  useEffect(() => {
    setRecordings([]);
    setPrepJobs([]);
    setRecordingError(null);
    if (!activeProject) {
      setRecordingStatus("idle");
      return;
    }

    void Promise.all([loadRecordings(activeProject.id), loadPreparationJobs(activeProject.id)]);
  }, [activeProject?.id]);

  const activePrepJob = useMemo(
    () => prepJobs.find((job) => job.status === "pending" || job.status === "running") ?? null,
    [prepJobs],
  );
  useEffect(() => {
    if (!activeProject || !activePrepJob) {
      return;
    }

    const intervalId = window.setInterval(() => {
      void fetchProcessingJob(activePrepJob.id)
        .then(async (job) => {
          setPrepJobs((current) => [job, ...current.filter((entry) => entry.id !== job.id)]);
          if (job.status === "completed" || job.status === "failed") {
            await Promise.all([
              loadRecordings(activeProject.id),
              loadPreparationJobs(activeProject.id),
            ]);
            onRetryProjects();
          }
        })
        .catch(() => {
          void loadPreparationJobs(activeProject.id);
        });
    }, 2000);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [activeProject, activePrepJob?.id, onRetryProjects]);

  const rawRecordings = useMemo(
    () => recordings.filter((recording) => !recording.parent_recording_id),
    [recordings],
  );
  const preparedRecordings = useMemo(
    () => recordings.filter((recording) => recording.parent_recording_id),
    [recordings],
  );
  const latestCompletedPrepJob = useMemo(
    () =>
      prepJobs.find((job) => job.status === "completed" && job.output_payload?.output_group_id) ??
      null,
    [prepJobs],
  );
  const activePreparedOutputGroupId =
    activeProject?.active_prepared_output_group_id ??
    (typeof latestCompletedPrepJob?.output_payload?.output_group_id === "string"
      ? latestCompletedPrepJob.output_payload.output_group_id
      : null);
  const latestPreparedGroup = useMemo(() => {
    if (!activePreparedOutputGroupId) {
      return null;
    }
    const groups = new Map<string, SourceRecordingQueue[]>();
    for (const recording of preparedRecordings) {
      const recipe = parsePreparationRecipe(recording);
      if (!recipe?.outputGroupId) {
        continue;
      }
      groups.set(recipe.outputGroupId, [...(groups.get(recipe.outputGroupId) ?? []), recording]);
    }
    const group = groups.get(activePreparedOutputGroupId);
    return group ? ([activePreparedOutputGroupId, group] satisfies [string, SourceRecordingQueue[]]) : null;
  }, [activePreparedOutputGroupId, preparedRecordings]);

  const latestPreparedSettings =
    (latestCompletedPrepJob?.output_payload?.settings as PreparationSettings | undefined) ??
    (latestPreparedGroup ? parsePreparationRecipe(latestPreparedGroup[1][0])?.settings ?? null : null);
  const totalDuration = rawRecordings.reduce(
    (sum, recording) => sum + recording.duration_seconds,
    0,
  );
  const sampleRates = Array.from(new Set(rawRecordings.map((recording) => recording.sample_rate))).sort(
    (left, right) => left - right,
  );
  const channelCounts = Array.from(new Set(rawRecordings.map((recording) => recording.num_channels))).sort(
    (left, right) => left - right,
  );
  const prepIsStale =
    latestPreparedSettings !== null &&
    stableSettingsKey(latestPreparedSettings) !== stableSettingsKey(prepSettings);
  const warnings = [
    rawRecordings.length === 0 ? "No raw recordings imported." : null,
    sampleRates.length > 1 ? "Mixed sample rates across imported recordings." : null,
    channelCounts.length > 1 ? "Mixed channel counts across imported recordings." : null,
    prepIsStale ? "Preparation settings changed after the latest prepared output." : null,
  ].filter((warning): warning is string => Boolean(warning));
  async function handleRunPreparation() {
    if (!activeProject) {
      return;
    }

    try {
      const result = await runProjectPreparation(activeProject.id, prepSettings);
      setPrepJobs((current) => [result.job, ...current.filter((job) => job.id !== result.job.id)]);
    } catch (error) {
      const failedAt = new Date().toISOString();
      setPrepJobs((current) => [
        {
          id: `prep-request-failed-${Date.now()}`,
          kind: "preprocess",
          status: "failed",
          input_payload: prepSettings,
          output_payload: null,
          error_message: getErrorMessage(error, "Preparation request failed."),
          created_at: failedAt,
          completed_at: failedAt,
        },
        ...current,
      ]);
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
        message="Create or select a project before inspecting raw recordings and preparation state."
      />
    );
  }

  if (recordingStatus === "loading") {
    return <WorkspaceStatePanel title="Loading recordings" message="Reading imported source metadata." />;
  }

  if (recordingStatus === "error") {
    return (
      <WorkspaceStatePanel
        title="Recordings unavailable"
        message={recordingError ?? "Source recordings could not be loaded."}
        actionLabel="Retry recordings"
        onAction={() => void loadRecordings(activeProject.id)}
      />
    );
  }

  return (
    <section className="step-page pipeline-page overview-page">
      <div className="stage-layout">
        <aside className="stage-sidebar panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Page owns</p>
              <h3>Raw recordings</h3>
            </div>
          </div>
          <ul className="stage-list">
            <li>
              <strong>Imported source files</strong>
              <span>{rawRecordings.length} immutable raw recording(s).</span>
            </li>
            <li>
              <strong>Prepared derivatives</strong>
              <span>{preparedRecordings.length} derived recording(s) on disk.</span>
            </li>
            <li>
              <strong>Current project</strong>
              <span>{activeProject.name}</span>
            </li>
          </ul>
        </aside>

        <main className="stage-main">
          <section className="panel pipeline-hero">
            <p className="eyebrow">Overview scope</p>
            <h3>Source-level preparation</h3>
            <p>
              Overview summarizes imported raw recordings and creates explicit prepared derivatives.
              It does not classify slices, accept clips, or edit Lab review state.
            </p>
          </section>

          <section className="stats-grid overview-stats-grid">
            <article className="stat-card">
              <span>Total duration</span>
              <strong>{formatDuration(totalDuration)}</strong>
            </article>
            <article className="stat-card">
              <span>Recordings</span>
              <strong>{rawRecordings.length}</strong>
            </article>
            <article className="stat-card">
              <span>Sample rates</span>
              <strong>{formatSet(sampleRates)}</strong>
            </article>
            <article className="stat-card">
              <span>Channels</span>
              <strong>{formatSet(channelCounts)}</strong>
            </article>
          </section>

          <section className="panel overview-prep-panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Preparation controls</p>
                <h3>Derived dataset settings</h3>
              </div>
            </div>
            <div className="overview-control-grid">
              <label>
                <span>Target sample rate</span>
                <select
                  value={prepSettings.target_sample_rate ?? "original"}
                  onChange={(event) =>
                    setPrepSettings((current) => ({
                      ...current,
                      target_sample_rate:
                        event.target.value === "original" ? null : Number(event.target.value),
                    }))
                  }
                >
                  <option value="original">Keep original</option>
                  <option value="16000">16,000 Hz</option>
                  <option value="22050">22,050 Hz</option>
                  <option value="24000">24,000 Hz</option>
                  <option value="44100">44,100 Hz</option>
                  <option value="48000">48,000 Hz</option>
                </select>
              </label>

              <label>
                <span>Channel handling</span>
                <select
                  value={prepSettings.channel_mode}
                  onChange={(event) =>
                    setPrepSettings((current) => ({
                      ...current,
                      channel_mode: event.target.value as PreparationSettings["channel_mode"],
                    }))
                  }
                >
                  <option value="original">Keep original channels</option>
                  <option value="mono">Downmix to mono</option>
                  <option value="left">Use left channel</option>
                  <option value="right">Use right channel</option>
                </select>
              </label>
            </div>
            <button
              className="primary-button"
              type="button"
              onClick={handleRunPreparation}
              disabled={rawRecordings.length === 0 || Boolean(activePrepJob)}
            >
              Run preparation
            </button>
          </section>

          <JobActivityPanel
            title="Preparation activity"
            job={prepJobs[0] ? jobToActivity(prepJobs[0], "Preparation") : null}
          />
        </main>

        <aside className="stage-sidebar panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Status</p>
              <h3>Warnings and output</h3>
            </div>
          </div>
          <ul className="stage-list">
            {warnings.length > 0 ? (
              warnings.map((warning) => (
                <li key={warning}>
                  <strong>Warning</strong>
                  <span>{warning}</span>
                </li>
              ))
            ) : (
              <li>
                <strong>No technical warnings</strong>
                <span>Raw source metadata is internally consistent.</span>
              </li>
            )}
            <li>
              <strong>Prepared output</strong>
              <span>
                {latestPreparedGroup
                  ? `${latestPreparedGroup[1].length} recording(s) in ${latestPreparedGroup[0]}`
                  : "No prepared derivative generated yet."}
              </span>
            </li>
            <li>
              <strong>Source mutation</strong>
              <span>Preparation writes new derived recordings and preserves raw imports.</span>
            </li>
          </ul>
        </aside>
      </div>
    </section>
  );
}
