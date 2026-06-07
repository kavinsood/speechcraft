import { useEffect, useMemo, useState } from "react";
import {
  ApiError,
  buildCandidateReviewAudioUrl,
  fetchDatasetRunLog,
  fetchDatasetSlicerResults,
  fetchProjectDatasetRuns,
  refreshDatasetRun,
  rerunDatasetSlicer,
} from "../api";
import JobActivityPanel, { type JobActivity } from "../components/JobActivityPanel";
import { usePipelineContext } from "../pipeline/PipelineContext";
import { hasCandidateClipArtifacts, isSlicerInputReady } from "../pipeline/datasetRunHelpers";
import type { DatasetRun, DatasetRunLog, DatasetSlicerResults, Project } from "../types";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type Props = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
  onOpenQc: () => void;
};

const defaults: Record<string, number> = {
  cutpoint_left_word_edge_guard_ms: 30,
  cutpoint_min_gap_ms: 80,
  cutpoint_right_word_edge_guard_ms: 30,
  cutpoint_noise_margin_db: 6,
  cutpoint_frame_ms: 20,
  cutpoint_hop_ms: 10,
  oov_cut_guard_sec: 0.5,
  symbol_cut_guard_sec: 0.5,
  numeric_cut_guard_sec: 0.5,
  provisional_split_guard_sec: 0.5,
  candidate_min_clip_sec: 3,
  candidate_target_clip_sec: 8,
  candidate_max_clip_sec: 15,
};

const controls = [
  ["Left word-edge guard", "cutpoint_left_word_edge_guard_ms", "ms", 0, 500, 5],
  ["Minimum usable gap", "cutpoint_min_gap_ms", "ms", 0, 1000, 5],
  ["Right word-edge guard", "cutpoint_right_word_edge_guard_ms", "ms", 0, 500, 5],
  ["Noise-floor margin", "cutpoint_noise_margin_db", "dB", 0, 30, 0.5],
  ["RMS frame", "cutpoint_frame_ms", "ms", 5, 200, 5],
  ["RMS hop", "cutpoint_hop_ms", "ms", 1, 100, 1],
  ["OOV cut guard", "oov_cut_guard_sec", "sec", 0, 5, 0.05],
  ["Symbol cut guard", "symbol_cut_guard_sec", "sec", 0, 5, 0.05],
  ["Numeric cut guard", "numeric_cut_guard_sec", "sec", 0, 5, 0.05],
  ["Provisional seam guard", "provisional_split_guard_sec", "sec", 0, 5, 0.05],
  ["Minimum clip", "candidate_min_clip_sec", "sec", 1, 15, 0.5],
  ["Target clip", "candidate_target_clip_sec", "sec", 1, 20, 0.5],
  ["Maximum clip", "candidate_max_clip_sec", "sec", 1, 30, 0.5],
] as const;

function errorMessage(error: unknown, fallback: string) {
  return error instanceof ApiError || error instanceof Error ? error.message || fallback : fallback;
}

function count(summary: Record<string, unknown>, key: string): number {
  const value = Number(summary[key] ?? 0);
  return Number.isFinite(value) ? value : 0;
}

function textValue(row: Record<string, unknown>, key: string): string {
  const value = row[key];
  return typeof value === "string" ? value : "";
}

function activity(run: DatasetRun | null, log: DatasetRunLog | null): JobActivity | null {
  if (!run) return null;
  return {
    id: run.id,
    name: `SafeCutPoint slicer · ${run.id}`,
    type: "slicing",
    state: run.status === "running" ? "running" : run.status === "failed" ? "failed" : run.status === "completed" ? "completed" : "idle",
    startedAt: run.started_at,
    completedAt: run.completed_at,
    progressLabel: `${run.status} · ${run.stage.replace(/_/g, " ")}`,
    logs: (log?.text ?? "").split("\n").filter(Boolean).map((message, index) => ({ id: `${run.id}-${index}`, timestamp: String(index + 1).padStart(3, "0"), message })),
  };
}

export default function SlicerPage({ activeProject, projectLoadStatus, projectLoadError, onRetryProjects, onOpenQc }: Props) {
  const { selectedSlicerDatasetRunId, selectSlicerDatasetRun } = usePipelineContext();
  const [runs, setRuns] = useState<DatasetRun[]>([]);
  const [results, setResults] = useState<DatasetSlicerResults | null>(null);
  const [log, setLog] = useState<DatasetRunLog | null>(null);
  const [settings, setSettings] = useState(defaults);
  const [error, setError] = useState<string | null>(null);
  const [pollWarning, setPollWarning] = useState<string | null>(null);
  const [pollFailureCount, setPollFailureCount] = useState(0);
  const [busy, setBusy] = useState(false);
  const selectedRun = useMemo(
    () => runs.find((run) => run.id === selectedSlicerDatasetRunId) ?? null,
    [runs, selectedSlicerDatasetRunId],
  );

  async function loadRuns(projectId: string) {
    try {
      setRuns(await fetchProjectDatasetRuns(projectId));
    } catch (loadError) {
      setError(errorMessage(loadError, "Dataset runs could not be loaded."));
    }
  }

  async function loadSelected(runId: string, refresh = false, polling = false) {
    try {
      const [run, nextResults, nextLog] = await Promise.all([
        refresh ? refreshDatasetRun(runId) : Promise.resolve(runs.find((item) => item.id === runId) ?? null),
        fetchDatasetSlicerResults(runId),
        fetchDatasetRunLog(runId),
      ]);
      if (run) setRuns((current) => [run, ...current.filter((item) => item.id !== run.id)]);
      setResults(nextResults);
      setLog(nextLog);
      setPollWarning(null);
      setPollFailureCount(0);
    } catch (loadError) {
      const message = errorMessage(loadError, "Slicer results could not be loaded.");
      if (polling) {
        setPollWarning(message);
        setPollFailureCount((current) => Math.min(current + 1, 3));
      } else {
        setError(message);
      }
    }
  }

  useEffect(() => {
    setRuns([]);
    setResults(null);
    setLog(null);
    if (activeProject) void loadRuns(activeProject.id);
  }, [activeProject?.id]);

  useEffect(() => {
    if (selectedSlicerDatasetRunId) void loadSelected(selectedSlicerDatasetRunId);
    else {
      setResults(null);
      setLog(null);
    }
  }, [selectedSlicerDatasetRunId]);

  useEffect(() => {
    if (!selectedRun || selectedRun.status !== "running") return;
    const intervalMs = pollFailureCount === 0 ? 2000 : pollFailureCount === 1 ? 5000 : 10000;
    const timer = window.setInterval(() => void loadSelected(selectedRun.id, true, true), intervalMs);
    return () => window.clearInterval(timer);
  }, [selectedRun?.id, selectedRun?.status, pollFailureCount]);

  async function generateOrRegenerate() {
    if (!selectedRun) return;
    setBusy(true);
    setError(null);
    try {
      const run = await rerunDatasetSlicer(selectedRun.id, settings);
      setRuns((current) => [run, ...current.filter((item) => item.id !== run.id)]);
      await loadSelected(run.id, true);
    } catch (rerunError) {
      setError(errorMessage(rerunError, "SafeCutPoint slicer rerun failed."));
    } finally {
      setBusy(false);
    }
  }

  if (projectLoadStatus === "error") return <WorkspaceStatePanel title="Projects unavailable" message={projectLoadError ?? "Project load failed."} actionLabel="Retry" onAction={onRetryProjects} />;
  if (projectLoadStatus === "loading") return <WorkspaceStatePanel title="Loading projects" message="Fetching project context." />;
  if (!activeProject) return <WorkspaceStatePanel title="No project selected" message="Select a project before slicing." />;

  const safe = results?.safe_cutpoint_summary ?? {};
  const clips = results?.candidate_review_summary ?? {};
  const manifest = results?.candidate_review_manifest ?? [];
  const rejectionReasons = safe.rejection_reason_counts && typeof safe.rejection_reason_counts === "object" ? Object.entries(safe.rejection_reason_counts as Record<string, unknown>) : [];
  const slicerReady = isSlicerInputReady(selectedRun);
  const hasCandidates = hasCandidateClipArtifacts(selectedRun, results);
  const generateDisabled = !selectedRun || !slicerReady || selectedRun.status === "running" || busy;
  const generateLabel = busy
    ? "Starting..."
    : hasCandidates
      ? "Regenerate SafeCutPoints + assembly"
      : "Generate candidate clips";

  return (
    <section className="step-page dataset-slicer-page">
      {error ? <p className="shell-notice shell-notice-error">{error}</p> : null}
      {pollWarning ? <p className="shell-notice">{pollWarning}</p> : null}
      <div className="dataset-slicer-layout">
        <aside className="panel processing-sidebar">
          <div className="panel-header"><div><p className="eyebrow">Dataset runs</p><h3>Slicer inputs</h3></div></div>
          <div className="processing-run-list">
            {runs.map((run) => <button key={run.id} type="button" aria-pressed={selectedRun?.id === run.id} onClick={() => selectSlicerDatasetRun(run.id)}><strong>{run.id}</strong><span>{run.status} · {run.stage.replace(/_/g, " ")}</span></button>)}
            {runs.length === 0 ? <p>No dataset runs. Complete Processing first.</p> : null}
          </div>
        </aside>

        <main className="processing-main">
          <section className="panel processing-controls">
            <div className="panel-header"><div><p className="eyebrow">SafeCutPoint authority</p><h3>Acoustic cut and assembly controls</h3></div><span className="status-pill">{slicerReady ? "Alignment ready" : "Needs alignment"}</span></div>
            {!selectedRun ? (
              <p>Select a dataset run from the sidebar or use Open Slicer on a completed Processing run.</p>
            ) : slicerReady && !hasCandidates ? (
              <p>Alignment is ready. SafeCutPoints and candidate clips have not been generated yet.</p>
            ) : hasCandidates ? (
              <p>Existing candidate clips found for this run.</p>
            ) : (
              <p>Complete Processing through alignment QC before generating candidate clips.</p>
            )}
            <div className="processing-settings-grid">
              {controls.map(([label, key, unit, min, max, step]) => <label className="processing-setting" key={key}><span>{label}</span><span className="processing-input-row"><input type="number" value={settings[key]} min={min} max={max} step={step} onChange={(event) => setSettings((current) => ({ ...current, [key]: Number(event.target.value) }))} /><small>{unit}</small></span></label>)}
            </div>
            <div className="processing-actions">
              <button className="primary-button" type="button" disabled={generateDisabled} onClick={() => void generateOrRegenerate()}>{generateLabel}</button>
              <span>Only slicer artifacts are regenerated. ASR and MFA remain untouched.</span>
            </div>
          </section>

          <section className="dataset-slicer-stats">
            <article className="panel pipeline-card"><p className="eyebrow">SafeCutPoints</p><h3>{count(safe, "accepted_cutpoints")}</h3><p>{count(safe, "rejected_cutpoint_candidates")} rejected · {(count(safe, "acceptance_rate") * 100).toFixed(1)}% accepted</p></article>
            <article className="panel pipeline-card"><p className="eyebrow">Candidate yield</p><h3>{count(clips, "candidate_review_clips")} clips</h3><p>{count(clips, "total_duration_sec").toFixed(1)} sec · {count(clips, "clips_needing_review")} review flagged</p></article>
            <article className="panel pipeline-card"><p className="eyebrow">Rejected spans</p><h3>{count(clips, "rejected_spans")}</h3><p>Buffers or spans not assembled automatically.</p></article>
          </section>

          <JobActivityPanel title="Slicer terminal" job={activity(selectedRun, log)} maxLogLines={500} />

          <section className="panel dataset-slicer-reasons">
            <div className="panel-header"><div><p className="eyebrow">Diagnostics</p><h3>SafeCutPoint rejection reasons</h3></div></div>
            <div>{rejectionReasons.length ? rejectionReasons.sort((a, b) => Number(b[1]) - Number(a[1])).map(([reason, value]) => <p key={reason}><strong>{reason.replace(/_/g, " ")}</strong><span>{String(value)}</span></p>) : <p>No SafeCutPoint diagnostics yet.</p>}</div>
          </section>

          <section className="panel dataset-slicer-candidates">
            <div className="panel-header"><div><p className="eyebrow">Candidate review clips</p><h3>Listening grid</h3></div></div>
            {!selectedRun ? (
              <p>Select a dataset run to inspect candidate clips.</p>
            ) : manifest.length === 0 ? (
              <p>No candidate clips generated yet.</p>
            ) : (
              <div className="processing-run-list">
                {manifest.map((clip) => {
                  const clipId = textValue(clip, "id");
                  const text = textValue(clip, "training_text");
                  const needsReview = Boolean(clip.needs_review);
                  const durationSec = Number(clip.duration_sec ?? 0);
                  return (
                    <article key={clipId} className="panel">
                      <p className="eyebrow">{clipId}</p>
                      <p><strong>{durationSec.toFixed(2)} sec</strong>{needsReview ? " · needs review" : ""}</p>
                      <audio controls preload="none" src={buildCandidateReviewAudioUrl(selectedRun.id, clipId)} />
                      <p>{text || "No transcript text recorded."}</p>
                    </article>
                  );
                })}
              </div>
            )}
          </section>
        </main>

        <aside className="panel processing-sidebar">
          <div className="panel-header"><div><p className="eyebrow">Next stage</p><h3>QC handoff</h3></div></div>
          <p>Candidate review clips are not final training clips. QC and native-rate export remain separate stages.</p>
          <button className="primary-button" type="button" disabled={!selectedRun || manifest.length === 0 || selectedRun.status === "running"} onClick={onOpenQc}>Open QC</button>
        </aside>
      </div>
    </section>
  );
}
