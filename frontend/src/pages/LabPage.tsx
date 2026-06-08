import { type ReactNode, useEffect, useRef, useState } from "react";
import { API_BASE, fetchDatasetSlicerResults } from "../api";
import { usePipelineContext } from "../pipeline/PipelineContext";
import type { ClipLabItemRef, Project } from "../types";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type LabPageProps = {
  activeProject: Project | null;
  activeClipItem: ClipLabItemRef | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onActiveClipItemChange: (clipItem: ClipLabItemRef | null) => void;
  onRetryProjects: () => void;
  onHeaderActionsChange: (actions: ReactNode) => void;
};

type Decision = "accept" | "reject" | "needs_edit" | null;
type Clip = Record<string, unknown>;

const DECISION_LABELS: Record<NonNullable<Decision>, string> = {
  accept: "✓ Accept",
  reject: "✗ Reject",
  needs_edit: "✎ Needs edit",
};

export default function LabPage({ activeProject, projectLoadStatus, projectLoadError, onActiveClipItemChange, onRetryProjects, onHeaderActionsChange }: LabPageProps) {
  const { labHandoff, selectedLabDatasetRunId, selectedSlicerDatasetRunId } = usePipelineContext();
  const runId = labHandoff?.datasetRunId ?? selectedLabDatasetRunId ?? selectedSlicerDatasetRunId;

  const [clips, setClips] = useState<Clip[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [decisions, setDecisions] = useState<Record<string, Decision>>({});
  const [activeIndex, setActiveIndex] = useState(0);
  const audioRef = useRef<HTMLAudioElement>(null);

  const onActiveClipItemChangeRef = useRef(onActiveClipItemChange);
  const onHeaderActionsChangeRef = useRef(onHeaderActionsChange);
  onActiveClipItemChangeRef.current = onActiveClipItemChange;
  onHeaderActionsChangeRef.current = onHeaderActionsChange;

  useEffect(() => {
    onActiveClipItemChangeRef.current(null);
    onHeaderActionsChangeRef.current(null as ReactNode);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!runId) return;
    setLoading(true);
    setError(null);
    fetchDatasetSlicerResults(runId)
      .then((r) => {
        setClips(r.candidate_review_manifest as Clip[]);
        setActiveIndex(0);
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, [runId]);

  if (projectLoadStatus === "error") {
    return <WorkspaceStatePanel title="Projects unavailable" message={projectLoadError ?? "Project load failed."} actionLabel="Retry" onAction={onRetryProjects} />;
  }
  if (projectLoadStatus === "loading") {
    return <WorkspaceStatePanel title="Loading projects" message="Fetching project context." />;
  }
  if (!activeProject) {
    return <WorkspaceStatePanel title="No project selected" message="Select a project before opening Lab." />;
  }
  if (!runId) {
    return <WorkspaceStatePanel title="No run selected" message="Open Lab from the QC page." />;
  }

  const clip = clips[activeIndex] as Clip | undefined;
  const decided = Object.values(decisions).filter(Boolean).length;
  const accepted = Object.values(decisions).filter((d) => d === "accept").length;
  const rejected = Object.values(decisions).filter((d) => d === "reject").length;
  const needsEdit = Object.values(decisions).filter((d) => d === "needs_edit").length;

  function decide(d: Decision) {
    if (!clip) return;
    setDecisions((prev) => ({ ...prev, [String(clip.id)]: d }));
    if (activeIndex < clips.length - 1) setActiveIndex((i) => i + 1);
  }

  return (
    <section className="step-page pipeline-page lab-page-shell">
      <div className="legacy-demo-banner" style={{ background: "#7c3aed", color: "#fff", padding: "8px 16px", borderRadius: 6, marginBottom: 16, fontSize: 13 }}>
        <strong>Demo Clip Lab</strong> — decisions are local only, not persisted to backend.
      </div>

      {labHandoff && (
        <div className="pipeline-handoff-banner" style={{ marginBottom: 12, fontSize: 13, color: "#aaa" }}>
          QC handoff · run {labHandoff.datasetRunId.slice(0, 20)}… · filter {labHandoff.bucketFilter} · sort {labHandoff.sort}
        </div>
      )}

      <div style={{ display: "flex", gap: 24, marginBottom: 16, fontSize: 13 }}>
        <span>{clips.length} clips</span>
        <span style={{ color: "#22c55e" }}>✓ {accepted}</span>
        <span style={{ color: "#ef4444" }}>✗ {rejected}</span>
        <span style={{ color: "#f59e0b" }}>✎ {needsEdit}</span>
        <span style={{ color: "#888" }}>{clips.length - decided} unreviewed</span>
      </div>

      {loading && <p>Loading clips…</p>}
      {error && <p style={{ color: "red" }}>{error}</p>}

      {!loading && clips.length > 0 && (
        <div style={{ display: "flex", gap: 16 }}>
          {/* Queue sidebar */}
          <div style={{ width: 220, flexShrink: 0, overflowY: "auto", maxHeight: "70vh", border: "1px solid #333", borderRadius: 6, padding: 8 }}>
            {clips.map((c, i) => {
              const d = decisions[String(c.id)];
              return (
                <button
                  key={String(c.id)}
                  onClick={() => setActiveIndex(i)}
                  style={{
                    display: "block", width: "100%", textAlign: "left", padding: "6px 8px",
                    background: i === activeIndex ? "#1e293b" : "transparent",
                    border: "none", borderRadius: 4, cursor: "pointer", marginBottom: 2,
                    color: d === "accept" ? "#22c55e" : d === "reject" ? "#ef4444" : d === "needs_edit" ? "#f59e0b" : "#e2e8f0",
                    fontSize: 12,
                  }}
                >
                  <div style={{ fontFamily: "monospace", fontSize: 10, color: "#888" }}>{String(c.id).slice(0, 18)}</div>
                  <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {d ? DECISION_LABELS[d] : (c.needs_review ? "⚠ Review" : "· Candidate")}
                  </div>
                </button>
              );
            })}
          </div>

          {/* Main review pane */}
          {clip && (
            <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 16 }}>
              <div className="panel" style={{ padding: 20 }}>
                <p className="eyebrow" style={{ fontSize: 11 }}>
                  {activeIndex + 1} / {clips.length} · {String(clip.id)}
                </p>
                <h3 style={{ margin: "8px 0" }}>{String(clip.training_text ?? "")}</h3>
                <div style={{ display: "flex", gap: 16, fontSize: 13, color: "#888", marginBottom: 16 }}>
                  <span>{Number(clip.duration_sec).toFixed(2)}s</span>
                  {Boolean(clip.needs_review) && (
                    <span style={{ color: "#f59e0b" }}>
                      ⚠ {((clip.review_reason_codes as string[]) ?? []).join(", ")}
                    </span>
                  )}
                </div>

                <audio
                  ref={audioRef}
                  key={String(clip.id)}
                  src={`${API_BASE}/media/dataset-runs/${runId}/candidate-review/${String(clip.id)}.wav`}
                  controls
                  autoPlay
                  style={{ width: "100%", marginBottom: 16 }}
                />

                <div style={{ display: "flex", gap: 10 }}>
                  <button className="action-button" style={{ background: "#166534", color: "#fff" }} onClick={() => decide("accept")}>✓ Accept</button>
                  <button className="action-button" style={{ background: "#991b1b", color: "#fff" }} onClick={() => decide("reject")}>✗ Reject</button>
                  <button className="action-button" style={{ background: "#92400e", color: "#fff" }} onClick={() => decide("needs_edit")}>✎ Needs edit</button>
                  <button className="action-button" style={{ background: "#1e293b" }} onClick={() => setActiveIndex((i) => Math.min(i + 1, clips.length - 1))}>Skip →</button>
                </div>
              </div>
            </div>
          )}
        </div>
      )}
    </section>
  );
}
