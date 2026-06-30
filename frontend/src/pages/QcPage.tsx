import { useEffect, useMemo, useRef, useState } from "react";
import {
  ApiError,
  fetchDatasetQc,
  fetchProjectDatasetRuns,
  finalizeDatasetQc,
  generateDatasetQcScores,
  refreshDatasetRun,
  resolveApiUrl,
} from "../api";
import QcThresholdImpactCard from "../components/qc/QcThresholdImpactCard";
import { usePipelineContext } from "../pipeline/PipelineContext";
import {
  acceptedSample,
  bestRejected,
  type ClipWithMargins,
  combinedSummary,
  finalStatus,
  speakerCurve,
  thresholdStatus,
  worstKept,
  transcriptCurve,
} from "../qc/qcLogic";
import type { DatasetQcPayload, DatasetRun, ManualOverride, Project } from "../types";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type QcPageProps = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
};

type AuditSectionProps = {
  title: string;
  emptyMessage: string;
  entries: ClipWithMargins[];
  transcriptThreshold: number;
  speakerThreshold: number;
  overridesByClipId: Record<string, ManualOverride | null>;
  onOverrideChange: (clipId: string, override: ManualOverride | null) => void;
  onAudioPlay: (clipId: string, element: HTMLAudioElement) => void;
  tabs?: {
    label: string;
    active: boolean;
    onClick: () => void;
  }[];
};

function errorMessage(error: unknown, fallback: string): string {
  if (error instanceof ApiError) {
    return error.message;
  }
  if (error instanceof Error && error.message.trim()) {
    return error.message;
  }
  return fallback;
}

function formatStage(stage: string): string {
  return stage.replace(/_/g, " ");
}

function formatReason(reason: string): string {
  return reason.replace(/_/g, " ");
}

function formatDurationHms(totalSeconds: number): string {
  const roundedSeconds = Math.max(0, Math.round(totalSeconds));
  const hours = Math.floor(roundedSeconds / 3600);
  const minutes = Math.floor((roundedSeconds % 3600) / 60);
  const seconds = roundedSeconds % 60;

  if (hours > 0) {
    return `${hours}h ${minutes}m ${seconds}s`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  }
  return `${seconds}s`;
}

function summaryCardLabel(count: number, durationSec: number): string {
  return `${count} clips · ${formatDurationHms(durationSec)}`;
}

function scoreLabel(score: number | null): string {
  return score === null ? "--" : score.toFixed(2);
}

function canForceKeep(reasonCodes: string[]): boolean {
  return !reasonCodes.includes("missing_audio_file");
}

function currentThresholds(payload: DatasetQcPayload | null): {
  transcript: number;
  speaker: number;
} {
  if (!payload) {
    return { transcript: 85, speaker: 70 };
  }

  return {
    transcript: payload.finalized_thresholds?.transcript_match_min ?? payload.defaults.transcript_match_threshold,
    speaker: payload.finalized_thresholds?.speaker_check_min ?? payload.defaults.speaker_check_threshold,
  };
}

function canGenerateQcScores(payload: DatasetQcPayload | null): boolean {
  if (!payload || payload.ready) {
    return false;
  }
  return payload.missing_artifacts.includes("artifacts/transcript_qc.json")
    || payload.missing_artifacts.includes("artifacts/speaker_purity.json");
}

function AuditSection({
  title,
  emptyMessage,
  entries,
  transcriptThreshold,
  speakerThreshold,
  overridesByClipId,
  onOverrideChange,
  onAudioPlay,
  tabs = [],
}: AuditSectionProps) {
  return (
    <section className="panel qc-audit-section">
      <div className="panel-header">
        <div>
          <h3>{title}</h3>
        </div>
        {tabs.length > 0 ? (
          <div className="qc-audit-tabs" role="tablist" aria-label={`${title} modes`}>
            {tabs.map((tab) => (
              <button
                key={tab.label}
                className={tab.active ? "primary-button" : "ghost-button"}
                type="button"
                onClick={tab.onClick}
              >
                {tab.label}
              </button>
            ))}
          </div>
        ) : null}
      </div>
      {entries.length === 0 ? (
        <p>{emptyMessage}</p>
      ) : (
        <div className="qc-audit-list">
          <div className="qc-audit-list-head" aria-hidden="true">
            <span>Transcript</span>
            <span>T</span>
            <span>S</span>
            <span>Actions</span>
          </div>
          {entries.map((entry) => (
            <article
              key={entry.clip.clip_id}
              className="qc-audit-row"
            >
              <div className="qc-audit-row-main">
                <p className="qc-audit-transcript">{entry.clip.training_text || "No transcript text recorded."}</p>
                <span className="qc-audit-score">{scoreLabel(entry.clip.transcript_match)}</span>
                <span className="qc-audit-score">{scoreLabel(entry.clip.speaker_check)}</span>
                <div className="qc-audit-actions">
                  <button
                    className="ghost-button"
                    disabled={!canForceKeep(entry.clip.qc_reason_codes)}
                    type="button"
                    onClick={() => onOverrideChange(entry.clip.clip_id, "force_keep")}
                  >
                    Keep
                  </button>
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() => onOverrideChange(entry.clip.clip_id, "force_reject")}
                  >
                    Reject
                  </button>
                  {overridesByClipId[entry.clip.clip_id] !== undefined || entry.clip.manual_override ? (
                    <button
                      className="ghost-button"
                      type="button"
                      onClick={() => onOverrideChange(entry.clip.clip_id, null)}
                    >
                      Clear
                    </button>
                  ) : null}
                  {thresholdStatus(entry.clip, transcriptThreshold, speakerThreshold) !== finalStatus(
                    entry.clip,
                    transcriptThreshold,
                    speakerThreshold,
                    overridesByClipId[entry.clip.clip_id],
                  ) ? <span className="qc-audit-override-flag">Override</span> : null}
                </div>
              </div>
              <audio
                className="qc-audit-audio"
                controls
                preload="metadata"
                src={resolveApiUrl(entry.clip.audio_url)}
                onPlay={(event) => onAudioPlay(entry.clip.clip_id, event.currentTarget)}
              />
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

export default function QcPage({
  activeProject,
  projectLoadStatus,
  projectLoadError,
  onRetryProjects,
}: QcPageProps) {
  const { selectedQcDatasetRunId, selectedSlicerDatasetRunId, selectQcDatasetRun } = usePipelineContext();
  const runId = selectedQcDatasetRunId ?? selectedSlicerDatasetRunId;

  const [runs, setRuns] = useState<DatasetRun[]>([]);
  const [payload, setPayload] = useState<DatasetQcPayload | null>(null);
  const [qcLoadSettled, setQcLoadSettled] = useState(false);
  const [transcriptThreshold, setTranscriptThreshold] = useState(85);
  const [speakerThreshold, setSpeakerThreshold] = useState(70);
  const [draftTranscriptThreshold, setDraftTranscriptThreshold] = useState(85);
  const [draftSpeakerThreshold, setDraftSpeakerThreshold] = useState(70);
  const [overridesByClipId, setOverridesByClipId] = useState<Record<string, ManualOverride | null>>({});
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [runMenuOpen, setRunMenuOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [worstKeptMode, setWorstKeptMode] = useState<"closest" | "transcript" | "speaker">("closest");
  const [bestRejectedMode, setBestRejectedMode] = useState<
    "closest_to_passing" | "transcript_only" | "speaker_only"
  >("closest_to_passing");
  const currentAudioRef = useRef<HTMLAudioElement | null>(null);
  const selectedRun = useMemo(
    () => runs.find((run) => run.id === runId) ?? null,
    [runId, runs],
  );
  const runMenuRef = useRef<HTMLDivElement | null>(null);

  async function loadRuns(projectId: string) {
    try {
      const nextRuns = await fetchProjectDatasetRuns(projectId);
      setRuns(nextRuns);
    } catch (loadError) {
      setError(errorMessage(loadError, "Dataset runs could not be loaded."));
    }
  }

  async function loadQc(
    runToLoad: string,
    options: { refresh?: boolean; background?: boolean } = {},
  ) {
    const refresh = options.refresh ?? false;
    const background = options.background ?? false;
    if (!background) {
      setLoading(true);
      setQcLoadSettled(false);
      setError(null);
      setNotice(null);
    }
    try {
      const [nextRun, nextPayload] = await Promise.all([
        refresh ? refreshDatasetRun(runToLoad) : Promise.resolve(runs.find((run) => run.id === runToLoad) ?? null),
        fetchDatasetQc(runToLoad),
      ]);

      if (nextRun) {
        setRuns((current) => [nextRun, ...current.filter((run) => run.id !== nextRun.id)]);
      }

      setPayload(nextPayload);
      if (!background || nextPayload.ready) {
        const thresholds = currentThresholds(nextPayload);
        setTranscriptThreshold(thresholds.transcript);
        setSpeakerThreshold(thresholds.speaker);
        setDraftTranscriptThreshold(thresholds.transcript);
        setDraftSpeakerThreshold(thresholds.speaker);
        setOverridesByClipId({});
      }
      if (nextPayload.ready || (nextRun && nextRun.status !== "running")) {
        setGenerating(false);
      }
    } catch (loadError) {
      setError(errorMessage(loadError, "QC payload could not be loaded."));
      setGenerating(false);
    } finally {
      if (!background) {
        setLoading(false);
        setQcLoadSettled(true);
      }
    }
  }

  useEffect(() => {
    setRuns([]);
    setPayload(null);
    setQcLoadSettled(false);
    if (activeProject) {
      void loadRuns(activeProject.id);
    }
  }, [activeProject?.id]);

  useEffect(() => {
    if (runId) {
      void loadQc(runId);
    } else {
      setPayload(null);
      setQcLoadSettled(false);
      setDraftTranscriptThreshold(85);
      setDraftSpeakerThreshold(70);
      setOverridesByClipId({});
    }
  }, [runId]);

  useEffect(() => {
    if (!runMenuOpen) {
      return undefined;
    }
    function handlePointerDown(event: MouseEvent) {
      if (runMenuRef.current && !runMenuRef.current.contains(event.target as Node)) {
        setRunMenuOpen(false);
      }
    }
    window.addEventListener("mousedown", handlePointerDown);
    return () => window.removeEventListener("mousedown", handlePointerDown);
  }, [runMenuOpen]);

  const qcGenerationActive = generating || (
    selectedRun !== null
    && selectedRun.status === "running"
    && (selectedRun.stage === "transcript_qc" || selectedRun.stage === "speaker_purity")
  );

  useEffect(() => {
    if (!runId || !qcGenerationActive || loading || busy) {
      return undefined;
    }
    const timer = window.setTimeout(() => {
      void loadQc(runId, { refresh: true, background: true });
    }, 2000);
    return () => window.clearTimeout(timer);
  }, [busy, loading, qcGenerationActive, runId]);

  const clips = payload?.clips ?? [];
  const summary = useMemo(
    () => combinedSummary(clips, transcriptThreshold, speakerThreshold, overridesByClipId),
    [clips, overridesByClipId, speakerThreshold, transcriptThreshold],
  );
  const transcriptPoints = useMemo(() => transcriptCurve(clips), [clips]);
  const speakerPoints = useMemo(
    () => speakerCurve(clips),
    [clips],
  );
  const weakestAccepted = useMemo(
    () => worstKept(clips, transcriptThreshold, speakerThreshold, worstKeptMode, overridesByClipId).slice(0, 12),
    [clips, overridesByClipId, speakerThreshold, transcriptThreshold, worstKeptMode],
  );
  const closestRejected = useMemo(
    () =>
      bestRejected(clips, transcriptThreshold, speakerThreshold, bestRejectedMode, overridesByClipId).slice(0, 12),
    [bestRejectedMode, clips, overridesByClipId, speakerThreshold, transcriptThreshold],
  );
  const acceptanceSample = useMemo(
    () => acceptedSample(clips, transcriptThreshold, speakerThreshold, 12, overridesByClipId),
    [clips, overridesByClipId, speakerThreshold, transcriptThreshold],
  );

  function handleOverrideChange(clipId: string, override: ManualOverride | null) {
    setOverridesByClipId((current) => ({
      ...current,
      [clipId]: override,
    }));
  }

  function handleAudioPlay(_clipId: string, element: HTMLAudioElement) {
    if (currentAudioRef.current && currentAudioRef.current !== element) {
      currentAudioRef.current.pause();
    }
    currentAudioRef.current = element;
  }

  function commitTranscriptThreshold() {
    setTranscriptThreshold((current) => (
      current === draftTranscriptThreshold ? current : draftTranscriptThreshold
    ));
  }

  function commitSpeakerThreshold() {
    setSpeakerThreshold((current) => (
      current === draftSpeakerThreshold ? current : draftSpeakerThreshold
    ));
  }

  async function handleFinalize() {
    if (!runId) {
      return;
    }

    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await finalizeDatasetQc(runId, {
        thresholds: {
          transcript_match_min: transcriptThreshold,
          speaker_check_min: speakerThreshold,
        },
        manual_overrides: Object.entries(overridesByClipId)
          .filter(([, override]) => override === "force_keep" || override === "force_reject")
          .map(([clip_id, override]) => ({
            clip_id,
            override: override as ManualOverride,
          })),
      });
      setNotice(
        `Finalized ${response.summary.accepted_count} accepted and ${response.summary.rejected_count} rejected clips.`,
      );
      await loadQc(runId, { refresh: true });
    } catch (finalizeError) {
      setError(errorMessage(finalizeError, "Finalize QC failed."));
    } finally {
      setBusy(false);
    }
  }

  async function handleGenerateQcScores() {
    if (!runId) {
      return;
    }
    setGenerating(true);
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const nextRun = await generateDatasetQcScores(runId);
      setRuns((current) => [nextRun, ...current.filter((run) => run.id !== nextRun.id)]);
      setNotice("Running QC Scores for this dataset run.");
    } catch (generationError) {
      setGenerating(false);
      setError(errorMessage(generationError, "QC score generation failed."));
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
    return <WorkspaceStatePanel title="No project selected" message="Select a project before QC." />;
  }

  return (
    <section className="step-page pipeline-page qc-page">
      {error ? <p className="shell-notice shell-notice-error">{runId ? `Could not load Dataset QC for ${runId}: ${error}` : error}</p> : null}
      {notice ? <p className="shell-notice">{notice}</p> : null}

      <div className="qc-layout">
        <main className="processing-main">
          <section className="panel pipeline-hero qc-hero">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Dataset QC</p>
                <h3>Transcript-match and speaker-check thresholding</h3>
              </div>
              <div className="qc-hero-actions">
                <div className="qc-run-menu" ref={runMenuRef}>
                  <button
                    className="action-button"
                    disabled={runs.length === 0}
                    type="button"
                    onClick={() => setRunMenuOpen((current) => !current)}
                  >
                    {selectedRun ? `Run: ${selectedRun.id}` : "Dataset runs"}
                  </button>
                  {runMenuOpen ? (
                    <div className="qc-run-menu-popover">
                      {runs.length === 0 ? (
                        <p>No dataset runs yet.</p>
                      ) : (
                        <div className="qc-run-menu-list">
                          {runs.map((run) => (
                            <button
                              key={run.id}
                              className={selectedRun?.id === run.id ? "qc-run-menu-item qc-run-menu-item-active" : "qc-run-menu-item"}
                              type="button"
                              onClick={() => {
                                selectQcDatasetRun(run.id);
                                setRunMenuOpen(false);
                              }}
                            >
                              <strong>{run.id}</strong>
                              <span>{run.status} · {formatStage(run.stage)}</span>
                            </button>
                          ))}
                        </div>
                      )}
                    </div>
                  ) : null}
                </div>
                <button
                  className="action-button"
                  disabled={!runId || loading}
                  type="button"
                  onClick={() => {
                    if (runId) {
                      void loadQc(runId, { refresh: true });
                    }
                  }}
                >
                  Refresh
                </button>
                <button
                  className="primary-button"
                  disabled={!runId || !payload?.ready || busy}
                  type="button"
                  onClick={() => void handleFinalize()}
                >
                  {busy ? "Finalizing..." : "Finalize QC"}
                </button>
              </div>
            </div>

            {!runId ? (
              <p>Select a dataset run from the dataset-runs menu or open QC from Slicer.</p>
            ) : loading || !qcLoadSettled ? (
              <p>Loading QC artifacts…</p>
            ) : !payload ? (
              <p>QC payload failed to load. Check backend logs or retry.</p>
            ) : payload.ready ? (
              <>
                <p>
                  Live acceptance is a strict AND gate over transcript match and speaker check. Threshold changes stay local until finalize.
                </p>
                <div className="qc-summary-grid">
                  <article className="pipeline-card panel">
                    <p className="eyebrow">Accepted</p>
                    <h3>{summary.acceptedCount}</h3>
                    <p>{summaryCardLabel(summary.acceptedCount, summary.acceptedDurationSec)}</p>
                  </article>
                  <article className="pipeline-card panel">
                    <p className="eyebrow">Rejected</p>
                    <h3>{summary.rejectedCount}</h3>
                    <p>{summaryCardLabel(summary.rejectedCount, summary.rejectedDurationSec)}</p>
                  </article>
                  <article className="pipeline-card panel">
                    <p className="eyebrow">Overrides</p>
                    <h3>{Object.values(overridesByClipId).filter((value) => value !== undefined).length}</h3>
                    <p>Local changes waiting for finalize.</p>
                  </article>
                  <article className="pipeline-card panel">
                    <p className="eyebrow">State</p>
                    <h3>{payload.finalized ? "Finalized" : "Draft"}</h3>
                    <p>
                      {payload.finalized_thresholds
                        ? `Saved ${payload.finalized_thresholds.transcript_match_min}/${payload.finalized_thresholds.speaker_check_min}`
                        : `Default ${payload.defaults.transcript_match_threshold}/${payload.defaults.speaker_check_threshold}`}
                    </p>
                  </article>
                </div>
              </>
            ) : (
              <>
                <p>QC is not ready for this run yet.</p>
                {payload.missing_artifacts.length > 0 ? (
                  <div className="qc-artifact-block">
                    <strong>Missing artifacts</strong>
                    <ul>
                      {payload.missing_artifacts.map((artifact) => (
                        <li key={artifact}>{artifact}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}
                {canGenerateQcScores(payload) ? (
                  <div className="qc-artifact-block">
                    <strong>QC scores missing</strong>
                    <p>Run Transcript Match and Speaker Check scoring for this dataset run.</p>
                    <button
                      className="primary-button"
                      disabled={!runId || busy}
                      type="button"
                      onClick={() => void handleGenerateQcScores()}
                    >
                      {generating ? "Running QC Scores..." : "Run QC Scores"}
                    </button>
                  </div>
                ) : null}
                {payload.invalid_artifacts.length > 0 ? (
                  <div className="qc-artifact-block">
                    <strong>Invalid artifacts</strong>
                    <ul>
                      {payload.invalid_artifacts.map((artifact) => (
                        <li key={artifact}>{artifact}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}
              </>
            )}
          </section>

          {payload?.ready ? (
            <>
              <section className="panel qc-threshold-panel">
                <div className="panel-header">
                  <div>
                    <p className="eyebrow">Threshold impact</p>
                    <h3>How thresholds affect accepted duration</h3>
                    <p className="qc-threshold-panel-summary">
                      Current accepted set: {formatDurationHms(summary.acceptedDurationSec)} · {summary.acceptedCount} clips
                    </p>
                  </div>
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() => {
                      const thresholds = currentThresholds(payload);
                      setTranscriptThreshold(thresholds.transcript);
                      setSpeakerThreshold(thresholds.speaker);
                      setDraftTranscriptThreshold(thresholds.transcript);
                      setDraftSpeakerThreshold(thresholds.speaker);
                    }}
                  >
                    Reset to saved values
                  </button>
                </div>
                <div className="qc-threshold-grid">
                  <QcThresholdImpactCard
                    label="Transcript Match"
                    threshold={draftTranscriptThreshold}
                    points={transcriptPoints}
                    accentClassName="qc-curve-transcript"
                    emptyLabel="No transcript QC scores yet."
                    explanation="Accepted duration if only Transcript Match threshold changes. Final acceptance still requires both Transcript Match and Speaker Check."
                    onDraftChange={setDraftTranscriptThreshold}
                    onCommit={commitTranscriptThreshold}
                  />
                  <QcThresholdImpactCard
                    label="Speaker Check"
                    threshold={draftSpeakerThreshold}
                    points={speakerPoints}
                    accentClassName="qc-curve-speaker"
                    emptyLabel="No speaker QC scores yet."
                    explanation="Accepted duration if only Speaker Check threshold changes. Final acceptance still requires both Transcript Match and Speaker Check."
                    onDraftChange={setDraftSpeakerThreshold}
                    onCommit={commitSpeakerThreshold}
                  />
                </div>
              </section>

              <div className="qc-audit-board">
                <AuditSection
                title="Worst kept"
                emptyMessage="No accepted clips at the current thresholds."
                entries={weakestAccepted}
                transcriptThreshold={transcriptThreshold}
                speakerThreshold={speakerThreshold}
                overridesByClipId={overridesByClipId}
                onOverrideChange={handleOverrideChange}
                onAudioPlay={handleAudioPlay}
                tabs={[
                  {
                    label: "Closest to threshold",
                    active: worstKeptMode === "closest",
                    onClick: () => setWorstKeptMode("closest"),
                  },
                  {
                    label: "Transcript risk",
                    active: worstKeptMode === "transcript",
                    onClick: () => setWorstKeptMode("transcript"),
                  },
                  {
                    label: "Speaker risk",
                    active: worstKeptMode === "speaker",
                    onClick: () => setWorstKeptMode("speaker"),
                  },
                ]}
                />

                <AuditSection
                  title="Best rejected"
                  emptyMessage="No rejected clips near the boundary."
                  entries={closestRejected}
                  transcriptThreshold={transcriptThreshold}
                  speakerThreshold={speakerThreshold}
                  overridesByClipId={overridesByClipId}
                  onOverrideChange={handleOverrideChange}
                  onAudioPlay={handleAudioPlay}
                  tabs={[
                    {
                      label: "Closest to passing",
                      active: bestRejectedMode === "closest_to_passing",
                      onClick: () => setBestRejectedMode("closest_to_passing"),
                    },
                    {
                      label: "Rejected by transcript",
                      active: bestRejectedMode === "transcript_only",
                      onClick: () => setBestRejectedMode("transcript_only"),
                    },
                    {
                      label: "Rejected by speaker",
                      active: bestRejectedMode === "speaker_only",
                      onClick: () => setBestRejectedMode("speaker_only"),
                    },
                  ]}
                />

                <AuditSection
                  title="Accepted sample"
                  emptyMessage="No accepted sample available."
                  entries={acceptanceSample}
                  transcriptThreshold={transcriptThreshold}
                  speakerThreshold={speakerThreshold}
                  overridesByClipId={overridesByClipId}
                  onOverrideChange={handleOverrideChange}
                  onAudioPlay={handleAudioPlay}
                />
              </div>
            </>
          ) : null}
        </main>
      </div>
    </section>
  );
}
