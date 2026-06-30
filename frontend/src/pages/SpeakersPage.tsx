import { useEffect, useMemo, useState } from "react";
import {
  ApiError,
  API_BASE,
  createProjectDatasetRun,
  fetchDatasetRunLog,
  fetchDatasetSpeakerResults,
  fetchProjectDatasetRuns,
  fetchProjectSourceRecordings,
  refreshDatasetRun,
  saveDatasetSpeakerSelection,
  startDatasetRun,
  buildSpeakerSampleAudioUrl,
} from "../api";
import JobActivityPanel, { type JobActivity } from "../components/JobActivityPanel";
import { usePipelineContext } from "../pipeline/PipelineContext";
import type {
  DatasetRun,
  DatasetRunLog,
  DatasetSpeakerResults,
  Project,
  SourceRecording,
} from "../types";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type Props = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
  onOpenProcessing: () => void;
  onOpenProcessingWithRun: (runId: string) => void;
};

function getErrorMessage(error: unknown, fallback: string): string {
  if (
    error instanceof TypeError &&
    /(NetworkError|Failed to fetch|Load failed|fetch resource)/i.test(error.message || "")
  ) {
    return `Backend API is offline at ${API_BASE}. Restart make dev-backend, then refresh.`;
  }
  if (error instanceof ApiError || error instanceof Error) {
    return error.message || fallback;
  }
  return fallback;
}

function formatDuration(totalSeconds: number): string {
  const safeSeconds = Math.max(0, Math.round(totalSeconds));
  const hours = Math.floor(safeSeconds / 3600);
  const minutes = Math.floor((safeSeconds % 3600) / 60);
  const seconds = safeSeconds % 60;
  const parts: string[] = [];
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0 || hours > 0) parts.push(`${minutes}m`);
  parts.push(`${seconds}s`);
  return parts.join(" ");
}

function sourceDisplayName(recording: SourceRecording, index: number): string {
  const displayName = recording.display_name?.trim();
  if (displayName && !/^source-[a-f0-9]+\.wav$/i.test(displayName)) return displayName;
  return `Source audio ${index + 1}`;
}

function runActivity(run: DatasetRun | null, log: DatasetRunLog | null): JobActivity | null {
  if (!run) return null;
  const lines = (log?.text ?? "")
    .split("\n")
    .filter(Boolean)
    .map((message, index) => ({
      id: `${run.id}-${index}`,
      timestamp: String(index + 1).padStart(3, "0"),
      message,
    }));
  return {
    id: run.id,
    name: `Speaker detection · ${run.stage.replace(/_/g, " ")}`,
    type: "processing",
    state:
      run.status === "pending"
        ? "idle"
        : run.status === "completed"
          ? "completed"
          : run.status === "failed"
            ? "failed"
            : "running",
    startedAt: run.started_at,
    completedAt: run.completed_at,
    progressLabel: `${run.status} · ${run.artifacts.length} indexed artifact(s)`,
    logs: lines,
  };
}

export default function SpeakersPage({
  activeProject,
  projectLoadStatus,
  projectLoadError,
  onRetryProjects,
  onOpenProcessing,
  onOpenProcessingWithRun,
}: Props) {
  const { selectedSpeakersRunId, selectSpeakersRun } = usePipelineContext();
  const [recordings, setRecordings] = useState<SourceRecording[]>([]);
  const [selectedRecordingIds, setSelectedRecordingIds] = useState<string[]>([]);
  const [runs, setRuns] = useState<DatasetRun[]>([]);
  const [speakerResults, setSpeakerResults] = useState<DatasetSpeakerResults | null>(null);
  const [log, setLog] = useState<DatasetRunLog | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pollWarning, setPollWarning] = useState<string | null>(null);
  const [pollFailureCount, setPollFailureCount] = useState(0);

  const selectedRun = useMemo(
    () => runs.find((run) => run.id === selectedSpeakersRunId) ?? null,
    [runs, selectedSpeakersRunId],
  );
  const runningRun = runs.find((run) => run.status === "running") ?? null;
  const pendingRun = runs.find((run) => run.status === "pending") ?? null;
  const activityRun = selectedRun?.status === "running" ? selectedRun : runningRun ?? selectedRun;
  const speakerSummary = speakerResults?.speaker_regions_summary ?? {};
  const perSpeaker = (speakerSummary.per_speaker ?? {}) as Record<
    string,
    { duration_sec?: number; segment_count?: number }
  >;
  const speakerIds = Object.keys(perSpeaker).sort();
  const selectedSpeakerId = speakerResults?.speaker_selection?.target_speaker_id ?? null;

  async function load(projectId: string) {
    setError(null);
    try {
      const [nextRecordings, nextRuns] = await Promise.all([
        fetchProjectSourceRecordings(projectId),
        fetchProjectDatasetRuns(projectId),
      ]);
      const multiRuns = nextRuns.filter((run) => run.input_summary.single_speaker === false);
      setRecordings(nextRecordings);
      setRuns(multiRuns);
      setSelectedRecordingIds((current) => {
        const stillValid = current.filter((id) => nextRecordings.some((recording) => recording.id === id));
        if (stillValid.length > 0) return stillValid;
        return nextRecordings.length > 0 ? [nextRecordings[0]!.id] : [];
      });
      if (selectedSpeakersRunId && !multiRuns.some((run) => run.id === selectedSpeakersRunId)) {
        selectSpeakersRun(multiRuns[0]?.id ?? null);
      }
    } catch (loadError) {
      setError(getErrorMessage(loadError, "Speaker selection state could not be loaded."));
    }
  }

  async function loadSelected(runId: string, refresh = false, polling = false) {
    try {
      const [run, results, nextLog] = await Promise.all([
        refresh ? refreshDatasetRun(runId) : Promise.resolve(runs.find((item) => item.id === runId) ?? null),
        fetchDatasetSpeakerResults(runId),
        fetchDatasetRunLog(runId),
      ]);
      if (run) setRuns((current) => [run, ...current.filter((item) => item.id !== run.id)]);
      setSpeakerResults(results);
      setLog(nextLog);
      setPollWarning(null);
      setPollFailureCount(0);
    } catch (loadError) {
      const message = getErrorMessage(loadError, "Speaker detection results could not be loaded.");
      if (polling) {
        setPollWarning(message);
        setPollFailureCount((current) => Math.min(current + 1, 3));
      } else {
        setError(message);
      }
    }
  }

  useEffect(() => {
    setRecordings([]);
    setSelectedRecordingIds([]);
    setRuns([]);
    setSpeakerResults(null);
    setLog(null);
    if (activeProject) {
      void load(activeProject.id);
    }
  }, [activeProject?.id]);

  useEffect(() => {
    if (selectedSpeakersRunId) {
      void loadSelected(selectedSpeakersRunId);
    } else {
      setSpeakerResults(null);
      setLog(null);
    }
  }, [selectedSpeakersRunId]);

  useEffect(() => {
    if (!selectedRun || selectedRun.status !== "running") return;
    const intervalMs = pollFailureCount === 0 ? 2000 : pollFailureCount === 1 ? 5000 : 10000;
    const timer = window.setInterval(() => void loadSelected(selectedRun.id, true, true), intervalMs);
    return () => window.clearInterval(timer);
  }, [selectedRun?.id, selectedRun?.status, pollFailureCount]);

  async function runSpeakerDetection() {
    if (!activeProject || selectedRecordingIds.length !== 1) return;
    setBusy(true);
    setError(null);
    try {
      const created = await createProjectDatasetRun(activeProject.id, {
        source_recording_ids: selectedRecordingIds,
        single_speaker: false,
        target_speaker_label: "",
        stop_after: "diarization",
        config: {},
      });
      setRuns((current) => [created, ...current.filter((entry) => entry.id !== created.id)]);
      selectSpeakersRun(created.id);
      const started = await startDatasetRun(created.id);
      setRuns((current) => [started, ...current.filter((entry) => entry.id !== started.id)]);
      await loadSelected(started.id, true);
    } catch (runError) {
      setError(getErrorMessage(runError, "Speaker detection could not be started."));
    } finally {
      setBusy(false);
    }
  }

  async function startPending(run: DatasetRun) {
    setBusy(true);
    setError(null);
    try {
      const started = await startDatasetRun(run.id);
      setRuns((current) => [started, ...current.filter((entry) => entry.id !== started.id)]);
      selectSpeakersRun(started.id);
      await loadSelected(started.id, true);
    } catch (startError) {
      setError(getErrorMessage(startError, "Pending diarization run could not start."));
    } finally {
      setBusy(false);
    }
  }

  async function chooseSpeaker(speakerId: string) {
    if (!selectedRun) return;
    setBusy(true);
    setError(null);
    try {
      const selection = await saveDatasetSpeakerSelection(selectedRun.id, speakerId);
      setSpeakerResults((current) =>
        current
          ? {
              ...current,
              speaker_selection: selection,
            }
          : current,
      );
      await loadSelected(selectedRun.id, true);
    } catch (selectionError) {
      setError(getErrorMessage(selectionError, "Speaker selection could not be saved."));
    } finally {
      setBusy(false);
    }
  }

  if (projectLoadStatus === "error") {
    return (
      <WorkspaceStatePanel
        title="Projects unavailable"
        message={projectLoadError ?? "Project load failed."}
        actionLabel="Retry"
        onAction={onRetryProjects}
      />
    );
  }
  if (projectLoadStatus === "loading") {
    return <WorkspaceStatePanel title="Loading projects" message="Fetching project context." />;
  }
  if (!activeProject) {
    return <WorkspaceStatePanel title="No project selected" message="Select a project before choosing speakers." />;
  }

  const canRunDetection = !busy && !runningRun && selectedRecordingIds.length === 1;

  return (
    <section className="step-page processing-page">
      {error ? <p className="shell-notice shell-notice-error">{error}</p> : null}
      {pollWarning ? <p className="shell-notice">{pollWarning}</p> : null}

      <div className="processing-layout">
        <aside className="panel processing-sidebar">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Sources</p>
              <h3>Raw WAV selection</h3>
            </div>
          </div>
          <div className="processing-source-list">
            {recordings.map((recording, index) => (
              <label key={recording.id}>
                <input
                  type="checkbox"
                  checked={selectedRecordingIds.includes(recording.id)}
                  onChange={() =>
                    setSelectedRecordingIds((current) =>
                      current.includes(recording.id)
                        ? current.filter((id) => id !== recording.id)
                        : [...current, recording.id],
                    )
                  }
                />
                <span>
                  <strong>{sourceDisplayName(recording, index)}</strong>
                  <small>
                    {recording.sample_rate} Hz · {recording.num_channels} ch ·{" "}
                    {formatDuration(recording.duration_seconds)}
                  </small>
                </span>
              </label>
            ))}
          </div>

          <div className="panel-header processing-run-header">
            <div>
              <p className="eyebrow">History</p>
              <h3>Speaker runs</h3>
            </div>
          </div>
          <div className="processing-run-list">
            {runs.map((run) => (
              <button
                key={run.id}
                type="button"
                aria-pressed={selectedSpeakersRunId === run.id}
                onClick={() => selectSpeakersRun(run.id)}
              >
                <strong>{run.id}</strong>
                <span>{run.status} · {run.stage.replace(/_/g, " ")}</span>
              </button>
            ))}
            {runs.length === 0 ? <p>No speaker detection runs yet.</p> : null}
          </div>
        </aside>

        <main className="processing-main">
          <section className="panel processing-controls">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Single-speaker path</p>
                <h3>No manual speaker choice needed</h3>
              </div>
              <span className="status-pill">Bypass diarization</span>
            </div>
            <p>
              If this recording is already one voice, skip speaker detection and go straight to
              Processing.
            </p>
            <div className="processing-actions">
              <button className="secondary-button" type="button" onClick={onOpenProcessing}>
                Continue to Processing
              </button>
              <span>Processing will create a normal single-speaker alignment run.</span>
            </div>
          </section>

          <section className="panel processing-controls">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Multi-speaker path</p>
                <h3>Run speaker detection and choose a target voice</h3>
              </div>
              <span className="status-pill">One source WAV at a time</span>
            </div>
            <p>
              VAD runs internally before diarization. Pick one source WAV, generate speaker samples,
              then choose the speaker you want to keep.
            </p>
            <div className="processing-actions">
              {selectedRun?.status === "pending" ? (
                <button
                  className="primary-button"
                  type="button"
                  disabled={busy || Boolean(runningRun)}
                  onClick={() => void startPending(selectedRun)}
                >
                  Start selected pending run
                </button>
              ) : null}
              <button
                className="primary-button"
                type="button"
                disabled={!canRunDetection}
                onClick={() => void runSpeakerDetection()}
              >
                {busy ? "Starting..." : "Run speaker detection"}
              </button>
              <span>
                {selectedRecordingIds.length === 0
                  ? "Select one source WAV."
                  : selectedRecordingIds.length > 1
                    ? "Multi-speaker detection currently supports exactly one source WAV."
                    : runningRun
                      ? "A speaker detection run is already active."
                      : "Selected source will stop after diarization."}
              </span>
            </div>
          </section>

          {selectedRun ? (
            <section className="panel processing-controls">
              <div className="panel-header">
                <div>
                  <p className="eyebrow">Detected speakers</p>
                  <h3>Listen and choose the target voice</h3>
                </div>
                <span className="status-pill">
                  {selectedRun.stage.replace(/_/g, " ")} · {selectedRun.status}
                </span>
              </div>
              {speakerIds.length === 0 ? (
                <p>No speaker cards yet. Complete speaker detection first.</p>
              ) : (
                <div className="processing-artifacts">
                  <ul>
                    {speakerIds.map((speakerId) => {
                      const summary = perSpeaker[speakerId] ?? {};
                      const samples = (speakerResults?.speaker_samples_manifest ?? []).filter(
                        (sample) => sample.speaker_id === speakerId,
                      );
                      const isSelected = selectedSpeakerId === speakerId;
                      return (
                        <li key={speakerId}>
                          <strong>{speakerId}</strong>
                          <span>
                            Duration: {formatDuration(Number(summary.duration_sec ?? 0))} · Segments:{" "}
                            {Number(summary.segment_count ?? 0)}
                          </span>
                          <div className="processing-source-list">
                            {samples.map((sample) => (
                              <label key={sample.sample_id}>
                                <span>
                                  <strong>Sample {sample.sample_id.split("_").slice(-1)[0]}</strong>
                                  <small>{formatDuration(sample.duration_sec)}</small>
                                  <audio controls preload="none" src={buildSpeakerSampleAudioUrl(selectedRun.id, sample.sample_id)}>
                                    <track kind="captions" />
                                  </audio>
                                </span>
                              </label>
                            ))}
                          </div>
                          <div className="processing-actions">
                            <button
                              className={isSelected ? "secondary-button" : "primary-button"}
                              type="button"
                              disabled={busy}
                              onClick={() => void chooseSpeaker(speakerId)}
                            >
                              {isSelected ? "Selected speaker" : "Choose this speaker"}
                            </button>
                          </div>
                        </li>
                      );
                    })}
                  </ul>
                </div>
              )}

              {selectedSpeakerId ? (
                <div className="processing-actions processing-handoff">
                  <p>
                    Selected target speaker: <strong>{selectedSpeakerId}</strong>
                  </p>
                  <button
                    className="secondary-button"
                    type="button"
                    onClick={() => onOpenProcessingWithRun(selectedRun.id)}
                  >
                    Continue to Processing
                  </button>
                </div>
              ) : null}
            </section>
          ) : null}

          <JobActivityPanel title="Speaker detection terminal" job={runActivity(activityRun, log)} maxLogLines={500} />
          <section className="panel processing-artifacts">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Environment</p>
                <h3>Backend target</h3>
              </div>
            </div>
            <p>API: {API_BASE}</p>
          </section>
        </main>
      </div>
    </section>
  );
}
