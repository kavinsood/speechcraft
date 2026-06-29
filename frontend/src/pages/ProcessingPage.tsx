import { useEffect, useMemo, useState } from "react";
import {
  ApiError,
  API_BASE,
  createProjectDatasetRun,
  fetchDatasetSpeakerResults,
  fetchDatasetPreflight,
  fetchDatasetRunLog,
  fetchProjectDatasetRuns,
  fetchProjectSourceRecordings,
  refreshDatasetRun,
  resumeDatasetRunProcessing,
  startDatasetRun,
} from "../api";
import JobActivityPanel, { type JobActivity } from "../components/JobActivityPanel";
import { usePipelineContext } from "../pipeline/PipelineContext";
import { isReadyForSlicerHandoff } from "../pipeline/datasetRunHelpers";
import type {
  DatasetPreflight,
  DatasetRun,
  DatasetRunLog,
  DatasetSpeakerResults,
  Project,
  SourceRecording,
} from "../types";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type ProcessingPageProps = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
  onOpenSpeakers?: () => void;
  onOpenSlicerWithRun: (runId: string) => void;
};

const LANGUAGE_OPTIONS = [
  { value: "auto", label: "Auto-detect (Whisper)" },
  { value: "en", label: "English" },
  { value: "es", label: "Spanish" },
  { value: "fr", label: "French" },
  { value: "de", label: "German" },
  { value: "it", label: "Italian" },
  { value: "pt", label: "Portuguese" },
] as const;

const WHISPER_MODEL_OPTIONS = [
  { value: "large-v3", label: "Large-v3 (slow, best accuracy)" },
  { value: "base", label: "Base (fast, good accuracy)" },
] as const;

const EXECUTION_TARGET_OPTIONS = [
  { value: "alignment_qc", label: "Full run (through alignment QC)" },
  { value: "mfa", label: "Stop after MFA alignment" },
  { value: "normalization", label: "Stop after transcription (ASR)" },
] as const;

function whisperModelForPreflight(modelSize: string): string {
  return modelSize === "base" ? "base" : "large-v3";
}

function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof ApiError || error instanceof Error) return error.message || fallback;
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

function launchBlockReason(input: {
  busy: boolean;
  runningRun: DatasetRun | null;
  pendingRun: DatasetRun | null;
  selectedCount: number;
  preflightLoading: boolean;
  preflightOk: boolean;
  preflightError: string | null;
}): string | null {
  if (input.busy) return "Starting dataset worker…";
  if (input.runningRun) return "A dataset worker is already active.";
  if (input.pendingRun) return "A pending run must be started before creating another.";
  if (input.selectedCount === 0) return "Select at least one source WAV in the sidebar.";
  if (input.preflightLoading) return "Waiting for dataset worker preflight.";
  if (!input.preflightOk) return input.preflightError || "Dataset worker preflight must pass before launching.";
  return null;
}

function runActivity(run: DatasetRun | null, log: DatasetRunLog | null): JobActivity | null {
  if (!run) return null;
  const lines = (log?.text ?? "").split("\n").filter(Boolean).map((message, index) => ({
    id: `${run.id}-${index}`,
    timestamp: String(index + 1).padStart(3, "0"),
    message,
  }));
  return {
    id: run.id,
    name: `Dataset worker · ${run.stage.replace(/_/g, " ")}`,
    type: "processing",
    state: run.status === "pending" ? "idle" : run.status === "completed" ? "completed" : run.status === "failed" ? "failed" : "running",
    startedAt: run.started_at,
    completedAt: run.completed_at,
    progressLabel: `${run.status} · ${run.artifacts.length} indexed artifact(s)`,
    logs: lines,
  };
}

export default function ProcessingPage({
  activeProject,
  projectLoadStatus,
  projectLoadError,
  onRetryProjects,
  onOpenSpeakers,
  onOpenSlicerWithRun,
}: ProcessingPageProps) {
  const { selectedSpeakersRunId, selectedProcessingRunId, selectProcessingRun } = usePipelineContext();
  const [recordings, setRecordings] = useState<SourceRecording[]>([]);
  const [selectedRecordingIds, setSelectedRecordingIds] = useState<string[]>([]);
  const [runs, setRuns] = useState<DatasetRun[]>([]);
  const [log, setLog] = useState<DatasetRunLog | null>(null);
  const [speakerResults, setSpeakerResults] = useState<DatasetSpeakerResults | null>(null);
  const [preflight, setPreflight] = useState<DatasetPreflight | null>(null);
  const [preflightLoading, setPreflightLoading] = useState(false);
  const [audioLanguage, setAudioLanguage] = useState("auto");
  const [whisperModelSize, setWhisperModelSize] = useState("large-v3");
  const [executionTarget, setExecutionTarget] = useState("alignment_qc");
  const [error, setError] = useState<string | null>(null);
  const [runRefreshWarning, setRunRefreshWarning] = useState<string | null>(null);
  const [pollFailureCount, setPollFailureCount] = useState(0);
  const [busy, setBusy] = useState(false);

  const selectedRun = useMemo(
    () => runs.find((run) => run.id === selectedProcessingRunId) ?? null,
    [runs, selectedProcessingRunId],
  );
  const runningRun = runs.find((run) => run.status === "running") ?? null;
  const pendingRun = runs.find((run) => run.status === "pending") ?? null;
  const activityRun = selectedRun?.status === "running" ? selectedRun : runningRun ?? selectedRun;
  const selectedRunIsMultiSpeaker = selectedRun?.input_summary.single_speaker === false;
  const selectedRunSpeakerChosen = Boolean(speakerResults?.speaker_selection?.selected);
  const followingDifferentRun = Boolean(
    runningRun && selectedRun && runningRun.id !== selectedRun.id,
  );
  const pollRunIds = useMemo(
    () => [...new Set([selectedProcessingRunId, runningRun?.id].filter(Boolean))] as string[],
    [selectedProcessingRunId, runningRun?.id],
  );

  async function load(projectId: string) {
    setError(null);
    try {
      const [nextRecordings, nextRuns] = await Promise.all([
        fetchProjectSourceRecordings(projectId),
        fetchProjectDatasetRuns(projectId),
      ]);
      setRecordings(nextRecordings);
      setRuns(nextRuns);
      setSpeakerResults(null);
      setSelectedRecordingIds((current) => {
        const stillValid = current.filter((id) => nextRecordings.some((recording) => recording.id === id));
        if (stillValid.length > 0) return stillValid;
        return nextRecordings.map((recording) => recording.id);
      });
      const nextSelectedRunId =
        selectedProcessingRunId && nextRuns.some((run) => run.id === selectedProcessingRunId)
          ? selectedProcessingRunId
          : selectedSpeakersRunId && nextRuns.some((run) => run.id === selectedSpeakersRunId)
            ? selectedSpeakersRunId
            : nextRuns[0]?.id ?? null;
      if (nextSelectedRunId !== selectedProcessingRunId) {
        selectProcessingRun(nextSelectedRunId);
      }
    } catch (loadError) {
      setError(getErrorMessage(loadError, "Processing state could not be loaded."));
    }
  }

  async function loadPreflight() {
    setPreflightLoading(true);
    try {
      const nextPreflight = await fetchDatasetPreflight({
        asrModel: whisperModelForPreflight(whisperModelSize),
      });
      setPreflight(nextPreflight);
    } catch (preflightError) {
      setPreflight({ ok: false, error: getErrorMessage(preflightError, "Dataset worker preflight failed.") });
    } finally {
      setPreflightLoading(false);
    }
  }

  async function refreshRun(runId: string, options?: { updateLog?: boolean; polling?: boolean }) {
    try {
      const nextRun = await refreshDatasetRun(runId);
      setRuns((current) => [nextRun, ...current.filter((run) => run.id !== nextRun.id)]);
      const shouldUpdateLog = options?.updateLog ?? runId === (activityRun?.id ?? selectedProcessingRunId);
      let hadLogError = false;
      if (shouldUpdateLog) {
        try {
          setLog(await fetchDatasetRunLog(runId));
        } catch (logError) {
          hadLogError = true;
          setRunRefreshWarning(getErrorMessage(logError, "Dataset worker log could not be loaded."));
          setLog(null);
        }
      }
      if (options?.polling && !hadLogError) {
        setRunRefreshWarning(null);
        setPollFailureCount(0);
      }
    } catch (runError) {
      const message = getErrorMessage(runError, "Dataset run could not be refreshed.");
      if (options?.polling) {
        setRunRefreshWarning(message);
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
    setLog(null);
    if (activeProject) {
      void load(activeProject.id);
    }
  }, [activeProject?.id]);

  useEffect(() => {
    if (activeProject) {
      void loadPreflight();
    }
  }, [activeProject?.id, whisperModelSize]);

  useEffect(() => {
    if (activityRun) void refreshRun(activityRun.id, { updateLog: true });
    else setLog(null);
  }, [activityRun?.id]);

  useEffect(() => {
    if (!selectedProcessingRunId && selectedSpeakersRunId && runs.some((run) => run.id === selectedSpeakersRunId)) {
      selectProcessingRun(selectedSpeakersRunId);
    }
  }, [runs, selectedProcessingRunId, selectedSpeakersRunId]);

  useEffect(() => {
    if (!selectedRunIsMultiSpeaker || !selectedRun) {
      setSpeakerResults(null);
      return;
    }

    void fetchDatasetSpeakerResults(selectedRun.id)
      .then((results) => setSpeakerResults(results))
      .catch(() => setSpeakerResults(null));
  }, [selectedRun?.id, selectedRunIsMultiSpeaker]);

  useEffect(() => {
    if (pollRunIds.length === 0) return;
    const refreshAll = () => {
      for (const runId of pollRunIds) {
        void refreshRun(runId, { updateLog: runId === activityRun?.id, polling: true });
      }
    };
    if (!runningRun) return;
    const intervalMs = pollFailureCount === 0 ? 2000 : pollFailureCount === 1 ? 5000 : 10000;
    const timer = window.setInterval(refreshAll, intervalMs);
    return () => window.clearInterval(timer);
  }, [pollRunIds.join("|"), runningRun?.id, activityRun?.id, pollFailureCount]);

  async function startPending(run: DatasetRun) {
    setBusy(true);
    setError(null);
    try {
      const started = await startDatasetRun(run.id);
      setRuns((current) => [started, ...current.filter((entry) => entry.id !== started.id)]);
      selectProcessingRun(started.id);
      await refreshRun(started.id, { updateLog: true });
    } catch (startError) {
      setError(getErrorMessage(startError, "Pending dataset run could not start."));
      setRuns((current) => [run, ...current.filter((entry) => entry.id !== run.id)]);
      selectProcessingRun(run.id);
    } finally {
      setBusy(false);
    }
  }

  async function continueMultiSpeakerRun(run: DatasetRun) {
    setBusy(true);
    setError(null);
    try {
      const started = await resumeDatasetRunProcessing(
        run.id,
        executionTarget as "buffers" | "normalization" | "mfa" | "alignment_qc",
      );
      setRuns((current) => [started, ...current.filter((entry) => entry.id !== started.id)]);
      selectProcessingRun(started.id);
      await refreshRun(started.id, { updateLog: true });
    } catch (resumeError) {
      setError(getErrorMessage(resumeError, "Selected-speaker processing could not continue."));
    } finally {
      setBusy(false);
    }
  }

  async function launch() {
    if (!activeProject || selectedRecordingIds.length === 0) return;
    setBusy(true);
    setError(null);
    try {
      const created = await createProjectDatasetRun(activeProject.id, {
        source_recording_ids: selectedRecordingIds,
        single_speaker: true,
        target_speaker_label: "speaker_0",
        stop_after: executionTarget,
        language: audioLanguage,
        whisper_model_size: whisperModelSize as "large-v3" | "base",
      });
      setRuns((current) => [created, ...current.filter((entry) => entry.id !== created.id)]);
      selectProcessingRun(created.id);
      await startPending(created);
    } catch (launchError) {
      setError(getErrorMessage(launchError, "Dataset run could not be created."));
    } finally {
      setBusy(false);
    }
  }

  if (projectLoadStatus === "error") return <WorkspaceStatePanel title="Projects unavailable" message={projectLoadError ?? "Project load failed."} actionLabel="Retry" onAction={onRetryProjects} />;
  if (projectLoadStatus === "loading") return <WorkspaceStatePanel title="Loading projects" message="Fetching project context." />;
  if (!activeProject) return <WorkspaceStatePanel title="No project selected" message="Select a project before processing audio." />;

  const preflightOk = preflight?.ok === true;
  const launchBlockedReason = launchBlockReason({
    busy,
    runningRun,
    pendingRun,
    selectedCount: selectedRecordingIds.length,
    preflightLoading,
    preflightOk,
    preflightError: preflight?.error ?? null,
  });
  const launchDisabled = launchBlockedReason !== null;
  const readyForSlicerHandoff = selectedRun ? isReadyForSlicerHandoff(selectedRun) : false;
  const canContinueMultiSpeaker =
    Boolean(selectedRun) &&
    selectedRunIsMultiSpeaker &&
    selectedRunSpeakerChosen &&
    selectedRun.status !== "running" &&
    !readyForSlicerHandoff;

  return (
    <section className="step-page processing-page">
      {error ? <p className="shell-notice shell-notice-error">{error}</p> : null}
      <div className="processing-topline">
        <div>
          <p className="eyebrow">Environment</p>
          <strong>{preflightLoading ? "Checking dataset worker" : preflight?.ok ? "Dataset worker ready" : "ASR model unavailable"}</strong>
          <span>
            {preflightLoading
              ? `Verifying ${whisperModelForPreflight(whisperModelSize)} can load before launch.`
              : preflight?.ok
                ? `Whisper ${whisperModelForPreflight(whisperModelSize)} and worker tools passed preflight.`
                : preflight?.error ?? "Run preflight to inspect the worker environment."}
          </span>
          <small>API: {API_BASE}</small>
        </div>
        <button className="secondary-button" type="button" onClick={() => { void load(activeProject.id); void loadPreflight(); }}>Refresh all</button>
      </div>

      <div className="processing-layout">
        <aside className="panel processing-sidebar">
          <div className="panel-header"><div><p className="eyebrow">Sources</p><h3>Raw WAV selection</h3></div></div>
          <div className="processing-source-list">
            {recordings.map((recording, index) => (
              <label key={recording.id}>
                <input type="checkbox" checked={selectedRecordingIds.includes(recording.id)} onChange={() => setSelectedRecordingIds((current) => current.includes(recording.id) ? current.filter((id) => id !== recording.id) : [...current, recording.id])} />
                <span><strong>{sourceDisplayName(recording, index)}</strong><small>{recording.sample_rate} Hz · {recording.num_channels} ch · {formatDuration(recording.duration_seconds)}</small><small>{recording.id}</small></span>
              </label>
            ))}
          </div>
          <div className="panel-header processing-run-header"><div><p className="eyebrow">History</p><h3>Dataset runs</h3></div></div>
          <div className="processing-run-list">
            {runs.map((run) => <button key={run.id} type="button" aria-pressed={selectedProcessingRunId === run.id} onClick={() => selectProcessingRun(run.id)}><strong>{run.stage.replace(/_/g, " ")}</strong><span>{run.status} · {run.artifacts.length} artifacts</span></button>)}
            {runs.length === 0 ? <p>No dataset runs yet.</p> : null}
          </div>
        </aside>

        <main className="processing-main">
          <section className="panel processing-controls">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Run setup</p>
                <h3>Dataset Processing Engine</h3>
              </div>
              <span className="status-pill">{selectedRunIsMultiSpeaker ? "Multi-speaker handoff" : "Single speaker"}</span>
            </div>
            <div className="processing-stage-strip">
              <span>Buffers</span><span>ASR</span><span>MFA</span><span>Alignment QC</span>
            </div>
            <div className="processing-quick-settings processing-operator-form">
              <label className="processing-setting" title="Tell the pipeline what language the speakers use. MFA models are chosen automatically.">
                <span>Audio language</span>
                <select aria-label="Audio language" value={audioLanguage} onChange={(event) => setAudioLanguage(event.target.value)}>
                  {LANGUAGE_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>{option.label}</option>
                  ))}
                </select>
              </label>
              <label className="processing-setting" title="Hardware tradeoff between speed and transcription accuracy.">
                <span>Whisper model size</span>
                <select aria-label="Whisper model size" value={whisperModelSize} onChange={(event) => setWhisperModelSize(event.target.value)}>
                  {WHISPER_MODEL_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>{option.label}</option>
                  ))}
                </select>
              </label>
              <label className="processing-setting" title="Run the full alignment pipeline or stop after an intermediate stage.">
                <span>Execution target</span>
                <select aria-label="Execution target" value={executionTarget} onChange={(event) => setExecutionTarget(event.target.value)}>
                  {EXECUTION_TARGET_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>{option.label}</option>
                  ))}
                </select>
              </label>
            </div>
            <p className="muted-copy processing-operator-note">
              Buffer limits, beam size, timeouts, MFA dictionary paths, and compute device are locked server-side.
            </p>
            {selectedRunIsMultiSpeaker ? (
              <div className="processing-actions processing-handoff">
                {selectedRunSpeakerChosen ? (
                  <p>
                    Selected target speaker:{" "}
                    <strong>{speakerResults?.speaker_selection?.target_speaker_id ?? "unknown"}</strong>
                  </p>
                ) : (
                  <p>Multi-speaker runs need a chosen target speaker before alignment can continue.</p>
                )}
                {selectedRunSpeakerChosen ? (
                  <button className="secondary-button" type="button" disabled={!canContinueMultiSpeaker || busy} onClick={() => void continueMultiSpeakerRun(selectedRun!)}>
                    Continue processing selected speaker
                  </button>
                ) : onOpenSpeakers ? (
                  <button className="secondary-button" type="button" onClick={onOpenSpeakers}>
                    Return to Speakers
                  </button>
                ) : null}
              </div>
            ) : null}
            {!selectedRunIsMultiSpeaker ? (
              <div className="processing-actions">
                {selectedRun?.status === "pending" ? (
                  <button className={`primary-button${busy ? " is-busy" : ""}`} type="button" disabled={busy || Boolean(runningRun) || preflightLoading || !preflightOk} onClick={() => void startPending(selectedRun)}>{busy ? "Starting..." : "Start selected pending run"}</button>
                ) : null}
                <button className={`primary-button${busy ? " is-busy" : ""}`} type="button" disabled={launchDisabled} onClick={() => void launch()}>{busy ? "Starting..." : "Create and start single-speaker run"}</button>
                <span>{launchBlockedReason ?? `${selectedRecordingIds.length} source WAV(s) selected for single-speaker processing.`}</span>
              </div>
            ) : null}
            {readyForSlicerHandoff ? (
              <div className="processing-actions processing-handoff">
                <p>Alignment QC completed. Generate candidate clips on the Slicer page.</p>
                <button className="secondary-button" type="button" onClick={() => onOpenSlicerWithRun(selectedRun!.id)}>Open Slicer with this run</button>
              </div>
            ) : null}
          </section>

          {followingDifferentRun ? (
            <p className="shell-notice">
              Active run <strong>{runningRun!.id}</strong> is still processing. Terminal follows the active run.
              {" "}
              <button className="secondary-button" type="button" onClick={() => selectProcessingRun(runningRun!.id)}>
                Follow active run
              </button>
            </p>
          ) : null}
          {runRefreshWarning ? <p className="shell-notice">{runRefreshWarning}</p> : null}
          <JobActivityPanel title="Dataset worker terminal" job={runActivity(activityRun, log)} maxLogLines={500} />
          {selectedRun ? <section className="panel processing-artifacts"><div className="panel-header"><div><p className="eyebrow">Indexed outputs</p><h3>Artifacts</h3></div><span>{selectedRun.artifacts.length}</span></div><ul>{selectedRun.artifacts.map((artifact) => <li key={artifact.id}><strong>{artifact.kind.replace(/_/g, " ")}</strong><span>{artifact.path}</span><small>{artifact.byte_size ?? 0} bytes</small></li>)}</ul></section> : null}
        </main>
      </div>
    </section>
  );
}
