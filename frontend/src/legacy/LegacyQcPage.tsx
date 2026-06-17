import { useEffect, useMemo, useState } from "react";
import { ApiError, createProjectQcRun, fetchDatasetSlicerResults, fetchProjectQcRuns, fetchQcRun } from "../api";
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
const HIDDEN_REJECT_THRESHOLD = 0.35;
const HISTOGRAM_BIN_COUNT = 56;

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
  if (result.reason_codes.length > 0 || result.bucket === "auto_rejected") {
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

function datasetClipReasonCodes(row: Record<string, unknown>): string[] {
  return [
    ...(((row.review_reason_codes as unknown[]) ?? []).filter((value): value is string => typeof value === "string")),
  ];
}

function clamp01(value: number): number {
  return Math.max(0, Math.min(1, value));
}

function combinedAsrScore(avgLogprob: number | null, noSpeechProb: number | null): number | null {
  if (avgLogprob === null && noSpeechProb === null) {
    return null;
  }
  const logprobScore = avgLogprob === null ? 0.5 : clamp01((avgLogprob + 1.5) / 1.5);
  const noSpeechScore = noSpeechProb === null ? 0.5 : clamp01(1 - noSpeechProb);
  return Number((0.7 * logprobScore + 0.3 * noSpeechScore).toFixed(3));
}

function syntheticQcScore(input: {
  asrScore: number | null;
  needsReview: boolean;
  fatalCount: number;
  warningCount: number;
  oovCount: number;
  numericHazardCount: number;
  symbolHazardCount: number;
  reviewReasonCount: number;
}): number {
  const {
    asrScore,
    needsReview,
    fatalCount,
    warningCount,
    oovCount,
    numericHazardCount,
    symbolHazardCount,
    reviewReasonCount,
  } = input;
  if (fatalCount > 0) {
    return Number(Math.max(0.08, 0.24 - fatalCount * 0.04).toFixed(3));
  }
  let score = asrScore ?? (needsReview ? 0.72 : 0.9);
  if (needsReview) score -= 0.06;
  score -= Math.min(0.18, warningCount * 0.035);
  score -= Math.min(0.12, oovCount * 0.06);
  score -= Math.min(0.12, numericHazardCount * 0.045);
  score -= Math.min(0.12, symbolHazardCount * 0.05);
  score -= Math.min(0.08, Math.max(0, reviewReasonCount - 1) * 0.025);
  return Number(clamp01(score).toFixed(3));
}

function buildSyntheticQcRun(
  projectId: string,
  slicerRunId: string,
  candidateRows: Array<Record<string, unknown>>,
  alignmentQcRows: Array<Record<string, unknown>>,
  transcriptRows: Array<Record<string, unknown>>,
  alignedWords: Array<Record<string, unknown>>,
  keepThreshold: number,
  rejectThreshold: number,
  preset: string,
): QcRun {
  const createdAt = new Date().toISOString();
  const runId = `dataset-qc-${slicerRunId}`;
  const qcByBuffer = new Map(alignmentQcRows.map((row) => [String(row.buffer_id ?? ""), row]));
  const transcriptByBuffer = new Map(transcriptRows.map((row) => [String(row.buffer_id ?? ""), row]));
  const wordById = new Map(alignedWords.map((row) => [String(row.id ?? ""), row]));
  const results: SliceQcResult[] = candidateRows.map((row, index) => {
    const durationSeconds = Number(row.duration_sec ?? 0) || 0;
    const reviewReasons = datasetClipReasonCodes(row);
    const needsReview = Boolean(row.needs_review) || reviewReasons.length > 0;
    const bufferId = String(row.buffer_id ?? "");
    const qcRow = qcByBuffer.get(bufferId) ?? {};
    const transcriptRow = transcriptByBuffer.get(bufferId) ?? {};
    const fatalFlags = ((qcRow.fatal_reason_codes as unknown[]) ?? []).filter((value): value is string => typeof value === "string");
    const warningFlags = ((qcRow.warning_reason_codes as unknown[]) ?? []).filter((value): value is string => typeof value === "string");
    const avgLogprobRaw = qcRow.asr_min_avg_logprob;
    const noSpeechProbRaw = qcRow.asr_max_no_speech_prob;
    const avgLogprob = typeof avgLogprobRaw === "number" && Number.isFinite(avgLogprobRaw) ? avgLogprobRaw : null;
    const noSpeechProb = typeof noSpeechProbRaw === "number" && Number.isFinite(noSpeechProbRaw) ? noSpeechProbRaw : null;
    const asrScore = combinedAsrScore(avgLogprob, noSpeechProb);
    const candidateWordIds = Array.isArray(row.word_ids) ? row.word_ids.map(String) : [];
    const wordRows = candidateWordIds.map((wordId) => wordById.get(wordId)).filter((value): value is Record<string, unknown> => Boolean(value));
    const oovCount = wordRows.filter((word) => Boolean(word.is_oov)).length;
    const numericHazardCount = wordRows.filter((word) => Boolean(word.contains_numeric)).length;
    const symbolHazardCount = wordRows.filter((word) => Boolean(word.contains_danger_symbol)).length;
    const aggregateScore = syntheticQcScore({
      asrScore,
      needsReview,
      fatalCount: fatalFlags.length,
      warningCount: warningFlags.length,
      oovCount,
      numericHazardCount,
      symbolHazardCount,
      reviewReasonCount: reviewReasons.length,
    });
    const bucket: QcBucket =
      fatalFlags.length > 0 ? "auto_rejected" : needsReview ? "needs_review" : "auto_kept";
    const sampleRate = Number(row.sample_rate ?? 16000) || 16000;
    const startSeconds = Number(row.source_start_sample ?? 0) / sampleRate;
    const endSeconds = Number(row.source_end_sample ?? 0) / sampleRate;
    return {
      id: `${runId}-${String(row.id ?? index)}`,
      qc_run_id: runId,
      slice_id: String(row.id ?? `candidate-${index}`),
      source_recording_id: String(row.source_audio_id ?? ""),
      source_order_index: index,
      source_start_seconds: Number.isFinite(startSeconds) ? startSeconds : null,
      source_end_seconds: Number.isFinite(endSeconds) ? endSeconds : null,
      aggregate_score: Number(aggregateScore.toFixed(3)),
      bucket,
      raw_metrics: {
        transcript_text: String(row.training_text ?? ""),
        duration_seconds: durationSeconds,
        word_count: Array.isArray(row.word_ids) ? row.word_ids.length : 0,
        needs_review: needsReview,
        review_reason_count: reviewReasons.length,
        buffer_warning_reason_count: Array.isArray(row.buffer_warning_reason_codes) ? row.buffer_warning_reason_codes.length : 0,
        asr_score: asrScore,
        asr_min_avg_logprob: avgLogprob,
        asr_max_no_speech_prob: noSpeechProb,
        alignment_qc_fatal_count: fatalFlags.length,
        alignment_qc_warning_count: warningFlags.length,
        alignment_qc_fatal_flags: fatalFlags,
        alignment_qc_warning_flags: warningFlags,
        oov_count: oovCount,
        numeric_hazard_count: numericHazardCount,
        symbol_hazard_count: symbolHazardCount,
        slicer_review_reason_codes: reviewReasons,
        transcript_language_probability:
          typeof transcriptRow.language_probability === "number" && Number.isFinite(transcriptRow.language_probability)
            ? transcriptRow.language_probability
            : null,
      },
      reason_codes: [...fatalFlags, ...reviewReasons],
      human_review_status: "unresolved",
      is_locked: false,
      created_at: createdAt,
    };
  });
  const bucketCounts = results.reduce<Partial<Record<QcBucket, number>>>((counts, result) => {
    counts[result.bucket] = (counts[result.bucket] ?? 0) + 1;
    return counts;
  }, {});
  return {
    id: runId,
    project_id: projectId,
    slicer_run_id: slicerRunId,
    status: "completed",
    threshold_config: {
      keep_threshold: keepThreshold,
      reject_threshold: rejectThreshold,
      preset,
    },
    slice_population_hash: slicerRunId,
    transcript_basis_hash: slicerRunId,
    audio_basis_hash: slicerRunId,
    is_stale: false,
    stale_reason: null,
    error_message: null,
    created_at: createdAt,
    completed_at: createdAt,
    result_count: results.length,
    bucket_counts: bucketCounts,
    results,
  };
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
        message: `Keep ${run.threshold_config.keep_threshold ?? "n/a"}`,
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
  const { selectedSlicerDatasetRunId } = usePipelineContext();
  // Legacy QC: use dataset run ID as the slicer run ID stand-in (not connected to old backend tables)
  const selectedSlicerRunId = selectedSlicerDatasetRunId;
  const [selectedQcRunId, setSelectedQcRunId] = useState<string | null>(null);
  function selectQcRun(id: string | null) { setSelectedQcRunId(id); }
  const [loadStatus, setLoadStatus] = useState<QcLoadStatus>("idle");
  const [loadError, setLoadError] = useState<string | null>(null);
  const [qcRuns, setQcRuns] = useState<QcRun[]>([]);
  const [activeRun, setActiveRun] = useState<QcRun | null>(null);
  const [keepThreshold, setKeepThreshold] = useState(0.72);
  const [preset, setPreset] = useState("balanced");
  const [bucketFilter, setBucketFilter] = useState<BucketFilter>("all");
  const [sortMode, setSortMode] = useState<QcSortMode>("source-order");
  const [runError, setRunError] = useState<string | null>(null);
  const [isRunningQc, setIsRunningQc] = useState(false);
  const [datasetFallbackMode, setDatasetFallbackMode] = useState(false);

  async function loadQcRuns(projectId: string, slicerRunId: string) {
    setLoadStatus("loading");
    setLoadError(null);
    try {
      const runs = await fetchProjectQcRuns(projectId, slicerRunId);
      setDatasetFallbackMode(false);
      setQcRuns(runs);
      setLoadStatus("ready");
      if (!selectedQcRunId && runs[0]) {
        selectQcRun(runs[0].id);
      }
    } catch (error) {
      try {
        const results = await fetchDatasetSlicerResults(slicerRunId);
        const syntheticRun = buildSyntheticQcRun(
          projectId,
          slicerRunId,
          results.candidate_review_manifest,
          results.alignment_qc_by_buffer ?? [],
          results.transcripts ?? [],
          results.aligned_words ?? [],
          keepThreshold,
          HIDDEN_REJECT_THRESHOLD,
          preset,
        );
        setDatasetFallbackMode(true);
        setQcRuns([syntheticRun]);
        setActiveRun(syntheticRun);
        setLoadStatus("ready");
        selectQcRun(syntheticRun.id);
      } catch (datasetError) {
        setQcRuns([]);
        setActiveRun(null);
        setLoadStatus("error");
        setLoadError(getErrorMessage(datasetError ?? error, "QC runs could not be loaded."));
      }
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
    if (datasetFallbackMode) {
      setActiveRun(qcRuns.find((run) => run.id === selectedQcRunId) ?? null);
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
  }, [datasetFallbackMode, qcRuns, selectedQcRunId]);

  const results = activeRun?.results ?? [];
  const visibleBucketCounts = useMemo(
    () => countBuckets(results, keepThreshold, HIDDEN_REJECT_THRESHOLD),
    [results, keepThreshold],
  );
  const visibleYield = results.length ? visibleBucketCounts.auto_kept / results.length : 0;
  const totalDuration = results.reduce((sum, result) => sum + getResultDuration(result), 0);
  const keptDuration = results.reduce((sum, result) => {
    return classifyVisibleBucket(result, keepThreshold, HIDDEN_REJECT_THRESHOLD) === "auto_kept"
      ? sum + getResultDuration(result)
      : sum;
  }, 0);
  const durationYield = totalDuration > 0 ? keptDuration / totalDuration : 0;
  const reviewedSnapshotCount = results.filter((result) => result.human_review_status && result.human_review_status !== "unresolved").length;

  const filteredResults = useMemo(() => {
    const bucket = filterToBucket[bucketFilter];
    const nextResults = bucket
      ? results.filter((result) => classifyVisibleBucket(result, keepThreshold, HIDDEN_REJECT_THRESHOLD) === bucket)
      : [...results];

    if (sortMode === "source-order") {
      nextResults.sort(compareSourceOrder);
    } else if (sortMode === "qc-score-ascending") {
      nextResults.sort((left, right) => left.aggregate_score - right.aggregate_score);
    } else if (sortMode === "qc-score-descending") {
      nextResults.sort((left, right) => right.aggregate_score - left.aggregate_score);
    }
    return nextResults;
  }, [bucketFilter, keepThreshold, results, sortMode]);

  const histogramBins = useMemo(() => {
    const scores = results.map((result) => result.aggregate_score).filter((value) => Number.isFinite(value));
    if (scores.length === 0) {
      return Array.from({ length: HISTOGRAM_BIN_COUNT }, (_, index) => ({
        label: `${(index / HISTOGRAM_BIN_COUNT).toFixed(2)}`,
        count: 0,
        start: index / HISTOGRAM_BIN_COUNT,
        end: (index + 1) / HISTOGRAM_BIN_COUNT,
        bucketCounts: { auto_kept: 0, needs_review: 0, auto_rejected: 0 } as Record<QcBucket, number>,
        dominantBucket: "needs_review" as QcBucket,
      }));
    }
    const rawMin = Math.min(...scores);
    const rawMax = Math.max(...scores);
    const padding = Math.max(0.02, (rawMax - rawMin) * 0.08);
    const domainMin = Math.max(0, rawMin - padding);
    const domainMax = Math.min(1, rawMax + padding);
    const width = Math.max(0.001, domainMax - domainMin);
    const bins = Array.from({ length: HISTOGRAM_BIN_COUNT }, (_, index) => {
      const start = domainMin + (width * index) / HISTOGRAM_BIN_COUNT;
      const end = domainMin + (width * (index + 1)) / HISTOGRAM_BIN_COUNT;
      return {
        label: `${start.toFixed(2)}`,
        count: 0,
        start,
        end,
        bucketCounts: { auto_kept: 0, needs_review: 0, auto_rejected: 0 } as Record<QcBucket, number>,
        dominantBucket: "needs_review" as QcBucket,
      };
    });
    results.forEach((result) => {
      const score = result.aggregate_score;
      const normalized = (score - domainMin) / width;
      const index = Math.max(0, Math.min(HISTOGRAM_BIN_COUNT - 1, Math.floor(normalized * HISTOGRAM_BIN_COUNT)));
      bins[index].count += 1;
      const bucket = classifyVisibleBucket(result, keepThreshold, HIDDEN_REJECT_THRESHOLD);
      bins[index].bucketCounts[bucket] += 1;
    });
    bins.forEach((bin) => {
      const ordered = (Object.entries(bin.bucketCounts) as Array<[QcBucket, number]>).sort((left, right) => right[1] - left[1]);
      bin.dominantBucket = ordered[0]?.[0] ?? "needs_review";
    });
    return bins.filter((bin) => bin.count > 0);
  }, [keepThreshold, results]);
  const histogramMax = Math.max(1, ...histogramBins.map((bin) => bin.count));
  const histogramTicks = useMemo(() => {
    const top = histogramMax;
    if (top <= 1) return [1];
    if (top <= 4) return Array.from({ length: top }, (_, index) => top - index);
    const mid = Math.max(1, Math.round(top / 2));
    return [top, mid, 1];
  }, [histogramMax]);
  const sourceTimelineResults = useMemo(() => [...results].sort(compareSourceOrder), [results]);

  async function handleRunQc() {
    if (!activeProject || !selectedSlicerRunId) {
      return;
    }
    setRunError(null);
    setIsRunningQc(true);
    try {
      if (datasetFallbackMode) {
        const results = await fetchDatasetSlicerResults(selectedSlicerRunId);
        const syntheticRun = buildSyntheticQcRun(
          activeProject.id,
          selectedSlicerRunId,
          results.candidate_review_manifest,
          results.alignment_qc_by_buffer ?? [],
          results.transcripts ?? [],
          results.aligned_words ?? [],
          keepThreshold,
          HIDDEN_REJECT_THRESHOLD,
          preset,
        );
        setActiveRun(syntheticRun);
        setQcRuns([syntheticRun]);
        selectQcRun(syntheticRun.id);
        return;
      }
      const run = await createProjectQcRun(activeProject.id, {
        slicer_run_id: selectedSlicerRunId,
        keep_threshold: keepThreshold,
        reject_threshold: HIDDEN_REJECT_THRESHOLD,
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
      datasetRunId: selectedSlicerRunId,
      qcRunId: selectedQcRunId,
      bucketFilter,
      sort: sortMode,
      keepThreshold,
      rejectThreshold: HIDDEN_REJECT_THRESHOLD,
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
            <p className="qc-threshold-note">
              Clips with fatal alignment issues are auto-rejected. Everything else stays auto-kept or needs review.
            </p>
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
                <h3>Keep line and source timeline</h3>
              </div>
            </div>
            <div className="qc-histogram-card">
              <div className="qc-histogram-head">
                <div>
                  <strong>Score buckets</strong>
                  <span>Higher bars mean more clips landed in that range.</span>
                </div>
                <div className="qc-keep-badge">Keep ≥ {formatPercent(keepThreshold)}</div>
              </div>
              <div className="qc-histogram" aria-label="QC score distribution">
                <div className="qc-histogram-yaxis" aria-hidden="true">
                  {histogramTicks.map((tick) => (
                    <span key={tick}>{tick}</span>
                  ))}
                </div>
                <div className="qc-histogram-bars">
                  {histogramBins.map((bin) => {
                    const aboveKeep = bin.end >= keepThreshold;
                    return (
                      <div
                        key={`${bin.label}-${bin.count}`}
                        className={`qc-histogram-bin qc-histogram-bin-${bin.dominantBucket} ${aboveKeep ? "qc-histogram-bin-keep" : ""}`}
                      >
                        <small>{bin.count}</small>
                        <span style={{ height: `${Math.max(12, (bin.count / histogramMax) * 100)}%` }} />
                        <label>{bin.label}</label>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
            <div className="qc-distribution-legend">
              <span><i className="qc-chip qc-bucket-auto_kept" /> auto-kept</span>
              <span><i className="qc-chip qc-bucket-needs_review" /> needs review</span>
              <span><i className="qc-chip qc-bucket-auto_rejected" /> hard reject</span>
            </div>
            <label className="qc-inline-threshold">
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
            <div className="qc-timeline-strip" aria-label="QC source-order timeline">
              {sourceTimelineResults.map((result) => {
                const bucket = classifyVisibleBucket(result, keepThreshold, HIDDEN_REJECT_THRESHOLD);
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
            </div>
            <div className="qc-preview-table">
              <div className="qc-preview-row qc-preview-header">
                <span>Transcript</span>
                <span>Score</span>
                <span>ASR</span>
                <span>Visible bucket</span>
                <span>Machine bucket</span>
                <span>Reasons</span>
              </div>
              {filteredResults.slice(0, 80).map((result) => {
                const visibleBucket = classifyVisibleBucket(result, keepThreshold, HIDDEN_REJECT_THRESHOLD);
                return (
                  <div
                    className="qc-preview-row"
                    key={result.id}
                  >
                    <span>{String(result.raw_metrics.transcript_text ?? result.slice_id)}</span>
                    <span>{result.aggregate_score.toFixed(3)}</span>
                    <span>{typeof result.raw_metrics.asr_score === "number" ? Number(result.raw_metrics.asr_score).toFixed(3) : "—"}</span>
                    <span className={`qc-bucket-label qc-bucket-${visibleBucket}`}>{bucketLabels[visibleBucket]}</span>
                    <span>{bucketLabels[result.bucket]}</span>
                    <span>{result.reason_codes.length ? result.reason_codes.join(", ") : "none"}</span>
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
                Keep {formatPercent(keepThreshold)}, {preset}
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
