import { useEffect, useMemo, useState } from "react";
import { ApiError, createProjectQcRun, fetchProjectQcRuns, fetchQcRun } from "../api";
import JobActivityPanel, { type JobActivity } from "../components/JobActivityPanel";
import { usePipelineContext, type LabHandoffContext } from "../pipeline/PipelineContext";
import type { Project, QcBucket, QcRun, SliceQcResult } from "../types";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type QcPageProps = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
  onOpenLab: (handoff: LabHandoffContext) => void;
};

type QcLoadStatus = "idle" | "loading" | "ready" | "error";
type BucketFilter = LabHandoffContext["bucketFilter"];
type QcSortMode = LabHandoffContext["sort"];

const bucketLabels: Record<QcBucket, string> = {
  auto_kept: "Auto-kept",
  needs_review: "Needs review",
  auto_rejected: "Auto-rejected",
};

const filterToBucket: Partial<Record<BucketFilter, QcBucket>> = {
  "auto-kept": "auto_kept",
  "needs-review": "needs_review",
  "auto-rejected": "auto_rejected",
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

function numericMetric(result: SliceQcResult, key: string): number {
  const value = result.raw_metrics[key];
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function formatPercent(value: number): string {
  return `${Math.round(value * 100)}%`;
}

function sourceOrderKey(result: SliceQcResult): [string, number, number, string] {
  return [
    result.source_recording_id ?? "",
    result.source_order_index ?? Number.MAX_SAFE_INTEGER,
    result.source_start_seconds ?? Number.MAX_SAFE_INTEGER,
    result.slice_id,
  ];
}

function compareSourceOrder(left: SliceQcResult, right: SliceQcResult): number {
  const leftKey = sourceOrderKey(left);
  const rightKey = sourceOrderKey(right);
  return (
    leftKey[0].localeCompare(rightKey[0]) ||
    leftKey[1] - rightKey[1] ||
    leftKey[2] - rightKey[2] ||
    leftKey[3].localeCompare(rightKey[3])
  );
}

function getResultDuration(result: SliceQcResult): number {
  const explicitDuration = numericMetric(result, "duration_seconds");
  if (explicitDuration > 0) {
    return explicitDuration;
  }
  const start = result.source_start_seconds;
  const end = result.source_end_seconds;
  return typeof start === "number" && typeof end === "number" && end > start ? end - start : 0;
}

function classifyVisibleBucket(result: SliceQcResult, keepThreshold: number, rejectThreshold: number): QcBucket {
  if (result.reason_codes.length > 0 || result.aggregate_score < rejectThreshold) {
    return "auto_rejected";
  }
  if (result.aggregate_score >= keepThreshold) {
    return "auto_kept";
  }
  return "needs_review";
}

function countBuckets(results: SliceQcResult[], keepThreshold: number, rejectThreshold: number) {
  return results.reduce<Record<QcBucket, number>>(
    (counts, result) => {
      const bucket = classifyVisibleBucket(result, keepThreshold, rejectThreshold);
      counts[bucket] += 1;
      return counts;
    },
    { auto_kept: 0, needs_review: 0, auto_rejected: 0 },
  );
}

function runToActivity(run: QcRun | null, isRunning: boolean, error: string | null): JobActivity | null {
  if (isRunning) {
    return {
      id: "qc-request-running",
      name: "QC run request",
      type: "qc",
      state: "running",
      startedAt: new Date().toISOString(),
      progressLabel: "Scoring selected slicer run",
      logs: [
        { id: "qc-request-1", timestamp: "00:00", message: "Submitting QC run to backend." },
        { id: "qc-request-2", timestamp: "00:01", message: "Waiting for persisted QC results." },
      ],
    };
  }

  if (error) {
    return {
      id: "qc-request-failed",
      name: "QC run request",
      type: "qc",
      state: "failed",
      startedAt: new Date().toISOString(),
      completedAt: new Date().toISOString(),
      progressLabel: "QC request failed",
      logs: [{ id: "qc-error", timestamp: "error", message: error }],
    };
  }

  if (!run) {
    return null;
  }

  return {
    id: run.id,
    name: `QC run ${run.id}`,
    type: "qc",
    state: run.status === "pending" ? "running" : run.status,
    startedAt: run.created_at,
    completedAt: run.completed_at,
    progressLabel: run.is_stale ? "QC results are stale" : `QC results persisted for ${run.result_count} slices`,
    logs: [
      { id: `${run.id}-scope`, timestamp: "scope", message: `Slicer run ${run.slicer_run_id}` },
      {
        id: `${run.id}-thresholds`,
        timestamp: "config",
        message: `Keep ${run.threshold_config.keep_threshold ?? "n/a"}, reject ${run.threshold_config.reject_threshold ?? "n/a"}`,
      },
      {
        id: `${run.id}-buckets`,
        timestamp: "result",
        message: `${run.bucket_counts.auto_kept ?? 0} kept, ${run.bucket_counts.needs_review ?? 0} review, ${run.bucket_counts.auto_rejected ?? 0} rejected`,
      },
    ],
  };
}

export default function QcPage({
  activeProject,
  projectLoadStatus,
  projectLoadError,
  onRetryProjects,
  onOpenLab,
}: QcPageProps) {
  const { selectedSlicerRunId, selectedQcRunId, selectQcRun } = usePipelineContext();
  const [loadStatus, setLoadStatus] = useState<QcLoadStatus>("idle");
  const [loadError, setLoadError] = useState<string | null>(null);
  const [qcRuns, setQcRuns] = useState<QcRun[]>([]);
  const [activeRun, setActiveRun] = useState<QcRun | null>(null);
  const [keepThreshold, setKeepThreshold] = useState(0.72);
  const [rejectThreshold, setRejectThreshold] = useState(0.35);
  const [preset, setPreset] = useState("balanced");
  const [bucketFilter, setBucketFilter] = useState<BucketFilter>("all");
  const [sortMode, setSortMode] = useState<QcSortMode>("source-order");
  const [showAdvancedMetrics, setShowAdvancedMetrics] = useState(false);
  const [runError, setRunError] = useState<string | null>(null);
  const [isRunningQc, setIsRunningQc] = useState(false);

  async function loadQcRuns(projectId: string, slicerRunId: string) {
    setLoadStatus("loading");
    setLoadError(null);
    try {
      const runs = await fetchProjectQcRuns(projectId, slicerRunId);
      setQcRuns(runs);
      setLoadStatus("ready");
      if (!selectedQcRunId && runs[0]) {
        selectQcRun(runs[0].id);
      }
    } catch (error) {
      setQcRuns([]);
      setActiveRun(null);
      setLoadStatus("error");
      setLoadError(getErrorMessage(error, "QC runs could not be loaded."));
    }
  }

  useEffect(() => {
    setQcRuns([]);
    setActiveRun(null);
    setRunError(null);
    if (!activeProject || !selectedSlicerRunId) {
      setLoadStatus("idle");
      return;
    }
    void loadQcRuns(activeProject.id, selectedSlicerRunId);
  }, [activeProject?.id, selectedSlicerRunId]);

  useEffect(() => {
    if (!selectedQcRunId) {
      setActiveRun(null);
      return;
    }
    const qcRunId = selectedQcRunId;
    let cancelled = false;
    async function loadRunDetail() {
      try {
        const run = await fetchQcRun(qcRunId);
        if (cancelled) {
          return;
        }
        setActiveRun(run);
        setKeepThreshold(Number(run.threshold_config.keep_threshold ?? 0.72));
        setRejectThreshold(Number(run.threshold_config.reject_threshold ?? 0.35));
        setPreset(String(run.threshold_config.preset ?? "balanced"));
      } catch (error) {
        if (!cancelled) {
          setRunError(getErrorMessage(error, "QC run detail could not be loaded."));
        }
      }
    }
    void loadRunDetail();
    return () => {
      cancelled = true;
    };
  }, [selectedQcRunId]);

  const results = activeRun?.results ?? [];
  const visibleBucketCounts = useMemo(
    () => countBuckets(results, keepThreshold, rejectThreshold),
    [results, keepThreshold, rejectThreshold],
  );
  const visibleYield = results.length ? visibleBucketCounts.auto_kept / results.length : 0;
  const totalDuration = results.reduce((sum, result) => sum + getResultDuration(result), 0);
  const keptDuration = results.reduce((sum, result) => {
    return classifyVisibleBucket(result, keepThreshold, rejectThreshold) === "auto_kept"
      ? sum + getResultDuration(result)
      : sum;
  }, 0);
  const durationYield = totalDuration > 0 ? keptDuration / totalDuration : 0;
  const reviewedSnapshotCount = results.filter((result) => result.human_review_status && result.human_review_status !== "unresolved").length;

  const filteredResults = useMemo(() => {
    const bucket = filterToBucket[bucketFilter];
    const nextResults = bucket
      ? results.filter((result) => classifyVisibleBucket(result, keepThreshold, rejectThreshold) === bucket)
      : [...results];

    if (sortMode === "source-order") {
      nextResults.sort(compareSourceOrder);
    } else if (sortMode === "qc-score-ascending") {
      nextResults.sort((left, right) => left.aggregate_score - right.aggregate_score);
    } else if (sortMode === "qc-score-descending") {
      nextResults.sort((left, right) => right.aggregate_score - left.aggregate_score);
    }
    return nextResults;
  }, [bucketFilter, keepThreshold, rejectThreshold, results, sortMode]);

  const histogramBins = useMemo(() => {
    const bins = Array.from({ length: 10 }, (_, index) => ({ label: `${index / 10}-${(index + 1) / 10}`, count: 0 }));
    for (const result of results) {
      const index = Math.max(0, Math.min(9, Math.floor(result.aggregate_score * 10)));
      bins[index].count += 1;
    }
    return bins;
  }, [results]);
  const histogramMax = Math.max(1, ...histogramBins.map((bin) => bin.count));
  const sourceTimelineResults = useMemo(() => [...results].sort(compareSourceOrder), [results]);

  async function handleRunQc() {
    if (!activeProject || !selectedSlicerRunId) {
      return;
    }
    setRunError(null);
    setIsRunningQc(true);
    try {
      const run = await createProjectQcRun(activeProject.id, {
        slicer_run_id: selectedSlicerRunId,
        keep_threshold: keepThreshold,
        reject_threshold: rejectThreshold,
        preset,
      });
      setActiveRun(run);
      setQcRuns((current) => [run, ...current.filter((entry) => entry.id !== run.id)]);
      selectQcRun(run.id);
    } catch (error) {
      setRunError(getErrorMessage(error, "QC run could not be created."));
    } finally {
      setIsRunningQc(false);
    }
  }

  function handleOpenLab() {
    if (!selectedSlicerRunId) {
      return;
    }
    onOpenLab({
      source: "qc",
      slicerRunId: selectedSlicerRunId,
      qcRunId: selectedQcRunId,
      bucketFilter,
      sort: sortMode,
      keepThreshold,
      rejectThreshold,
      preset,
    });
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
    return <WorkspaceStatePanel title="No project selected" message="Select a project and slicer run before running QC." />;
  }

  if (!selectedSlicerRunId) {
    return (
      <WorkspaceStatePanel
        title="No slicer run selected"
        message="Choose or create a slicer run before opening machine QC triage."
      />
    );
  }

  if (loadStatus === "loading" && qcRuns.length === 0) {
    return <WorkspaceStatePanel title="Loading QC runs" message="Reading QC run history." />;
  }

  if (loadStatus === "error") {
    return (
      <WorkspaceStatePanel
        title="QC runs unavailable"
        message={loadError ?? "QC runs could not be loaded."}
        actionLabel="Retry QC runs"
        onAction={() => void loadQcRuns(activeProject.id, selectedSlicerRunId)}
      />
    );
  }

  return (
    <section className="step-page pipeline-page qc-page">
      <div className="stage-layout">
        <aside className="stage-sidebar panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">QC runs</p>
              <h3>Machine triage</h3>
            </div>
          </div>
          <ul className="stage-list">
            <li>
              <strong>Slicer run</strong>
              <span>{selectedSlicerRunId}</span>
            </li>
            {qcRuns.length > 0 ? (
              qcRuns.map((run) => (
                <li key={run.id}>
                  <button
                    className="pipeline-list-button"
                    type="button"
                    onClick={() => selectQcRun(run.id)}
                    aria-pressed={selectedQcRunId === run.id}
                  >
                    <strong>{run.id}</strong>
                    <span>
                      {run.status} · {run.result_count} slice(s){run.is_stale ? " · stale" : ""}
                    </span>
                  </button>
                </li>
              ))
            ) : (
              <li>
                <strong>No QC runs</strong>
                <span>Run QC for the selected slicer run.</span>
              </li>
            )}
          </ul>
        </aside>

        <main className="stage-main">
          <section className="panel pipeline-hero qc-control-panel">
            <p className="eyebrow">Run QC</p>
            <h3>{activeRun ? activeRun.id : "No QC run selected"}</h3>
            <div className="qc-threshold-grid">
              <label>
                <span>Keep threshold</span>
                <input
                  type="range"
                  min="0"
                  max="1"
                  step="0.01"
                  value={keepThreshold}
                  onChange={(event) => setKeepThreshold(Number(event.target.value))}
                />
                <strong>{formatPercent(keepThreshold)}</strong>
              </label>
              <label>
                <span>Reject threshold</span>
                <input
                  type="range"
                  min="0"
                  max={keepThreshold}
                  step="0.01"
                  value={rejectThreshold}
                  onChange={(event) => setRejectThreshold(Number(event.target.value))}
                />
                <strong>{formatPercent(rejectThreshold)}</strong>
              </label>
              <label>
                <span>Preset</span>
                <select className="search-input" value={preset} onChange={(event) => setPreset(event.target.value)}>
                  <option value="balanced">Balanced</option>
                  <option value="strict">Strict</option>
                  <option value="lenient">Lenient</option>
                </select>
              </label>
            </div>
            <button className="primary-button" type="button" onClick={handleRunQc} disabled={isRunningQc}>
              Run QC
            </button>
            {runError ? <p className="shell-notice shell-notice-error">{runError}</p> : null}
            {activeRun?.is_stale ? (
              <p className="shell-notice shell-notice-warning">
                Stale QC: {activeRun.stale_reason ?? "source changed"}. Rerun QC before using these thresholds for a fresh Lab pass.
              </p>
            ) : null}
          </section>

          <section className="pipeline-card-grid qc-summary-grid">
            <article className="panel pipeline-card">
              <p className="eyebrow">Visible yield</p>
              <h3>{formatPercent(visibleYield)}</h3>
              <p>
                {visibleBucketCounts.auto_kept} of {results.length} slices currently above keep threshold.
                Duration yield {formatPercent(durationYield)}.
              </p>
            </article>
            <article className="panel pipeline-card">
              <p className="eyebrow">Machine buckets</p>
              <h3>{visibleBucketCounts.auto_kept} / {visibleBucketCounts.needs_review} / {visibleBucketCounts.auto_rejected}</h3>
              <p>Kept, needs review, rejected under the visible thresholds.</p>
            </article>
            <article className="panel pipeline-card">
              <p className="eyebrow">Review snapshot</p>
              <h3>{reviewedSnapshotCount} reviewed</h3>
              <p>{results.length - reviewedSnapshotCount} unresolved when this QC run was created.</p>
            </article>
            <article className="panel pipeline-card">
              <p className="eyebrow">Run state</p>
              <h3>{activeRun?.is_stale ? "Stale" : activeRun?.status ?? "No run"}</h3>
              <p>{activeRun?.completed_at ? new Date(activeRun.completed_at).toLocaleString() : "No completion timestamp"}</p>
            </article>
          </section>

          <section className="panel qc-visual-panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Distribution</p>
                <h3>Scores and source timeline</h3>
              </div>
            </div>
            <div className="qc-histogram" aria-label="QC score distribution">
              {histogramBins.map((bin) => (
                <div key={bin.label} className="qc-histogram-bin">
                  <span style={{ height: `${Math.max(8, (bin.count / histogramMax) * 100)}%` }} />
                  <small>{bin.count}</small>
                </div>
              ))}
            </div>
            <div className="qc-timeline-strip" aria-label="QC source-order timeline">
              {sourceTimelineResults.map((result) => {
                const bucket = classifyVisibleBucket(result, keepThreshold, rejectThreshold);
                return (
                  <span
                    key={result.id}
                    className={`qc-timeline-segment qc-bucket-${bucket}`}
                    style={{ flexGrow: Math.max(0.2, getResultDuration(result)) }}
                    title={`${result.source_recording_id ?? "source"} @ ${result.source_start_seconds ?? "?"}s: ${bucketLabels[bucket]}`}
                  />
                );
              })}
            </div>
          </section>

          <section className="panel qc-preview-panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Preview</p>
                <h3>Slice QC results</h3>
              </div>
              <label className="overview-checkbox-label">
                <input
                  type="checkbox"
                  checked={showAdvancedMetrics}
                  onChange={(event) => setShowAdvancedMetrics(event.target.checked)}
                />
                <span>Advanced metrics</span>
              </label>
            </div>
            <div className="qc-preview-table">
              <div className="qc-preview-row qc-preview-header">
                <span>Slice</span>
                <span>Score</span>
                <span>Visible bucket</span>
                <span>Machine bucket</span>
                <span>Review snapshot</span>
                <span>Reasons</span>
                {showAdvancedMetrics ? <span>Metrics</span> : null}
              </div>
              {filteredResults.slice(0, 80).map((result) => {
                const visibleBucket = classifyVisibleBucket(result, keepThreshold, rejectThreshold);
                const reviewed = result.human_review_status && result.human_review_status !== "unresolved";
                return (
                  <div
                    className={`qc-preview-row ${reviewed ? "qc-reviewed" : "qc-unreviewed"}`}
                    key={result.id}
                  >
                    <span>{result.slice_id}</span>
                    <span>{result.aggregate_score.toFixed(3)}</span>
                    <span className={`qc-bucket-label qc-bucket-${visibleBucket}`}>{bucketLabels[visibleBucket]}</span>
                    <span>{bucketLabels[result.bucket]}</span>
                    <span>{reviewed ? result.human_review_status : "snapshot unresolved"}</span>
                    <span>{result.reason_codes.length ? result.reason_codes.join(", ") : "none"}</span>
                    {showAdvancedMetrics ? (
                      <span>
                        {numericMetric(result, "duration_seconds").toFixed(2)}s · {numericMetric(result, "word_count")} words
                      </span>
                    ) : null}
                  </div>
                );
              })}
            </div>
          </section>

          <JobActivityPanel title="QC activity" job={runToActivity(activeRun, isRunningQc, runError)} />
        </main>

        <aside className="stage-sidebar panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Lab handoff</p>
              <h3>Transfer context</h3>
            </div>
          </div>
          <ul className="stage-list">
            <li>
              <strong>Bucket</strong>
              <select className="search-input" value={bucketFilter} onChange={(event) => setBucketFilter(event.target.value as BucketFilter)}>
                <option value="all">All</option>
                <option value="auto-kept">Auto-kept</option>
                <option value="needs-review">Needs review</option>
                <option value="auto-rejected">Auto-rejected</option>
              </select>
            </li>
            <li>
              <strong>Sort</strong>
              <select className="search-input" value={sortMode} onChange={(event) => setSortMode(event.target.value as QcSortMode)}>
                <option value="source-order">Source order</option>
                <option value="qc-score-ascending">QC score ascending</option>
                <option value="qc-score-descending">QC score descending</option>
              </select>
            </li>
            <li>
              <strong>Selected QC run</strong>
              <span>{selectedQcRunId ?? "Run or select QC before handoff"}</span>
            </li>
            <li>
              <strong>Threshold context</strong>
              <span>
                Keep {formatPercent(keepThreshold)}, reject {formatPercent(rejectThreshold)}, {preset}
              </span>
            </li>
            {activeRun?.is_stale ? (
              <li>
                <strong>Handoff blocked</strong>
                <span>Rerun QC before transferring threshold-driven filters into Lab.</span>
              </li>
            ) : null}
          </ul>
          <button
            className="primary-button pipeline-full-button"
            type="button"
            onClick={handleOpenLab}
            disabled={!activeRun || activeRun.is_stale}
          >
            Open Lab
          </button>
        </aside>
      </div>
    </section>
  );
}
