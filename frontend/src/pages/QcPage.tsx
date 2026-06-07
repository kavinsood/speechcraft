import { useEffect, useState } from "react";
import { API_BASE, fetchDatasetSlicerResults } from "../api";
import { usePipelineContext } from "../pipeline/PipelineContext";
import type { Project } from "../types";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type QcPageProps = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
  onOpenLab: (handoff: { source: "qc"; datasetRunId: string; qcRunId: string | null; bucketFilter: "all"; sort: "source-order"; keepThreshold: null; rejectThreshold: null; preset: null }) => void;
};

type Clip = Record<string, unknown>;

export default function QcPage({ activeProject, projectLoadStatus, projectLoadError, onRetryProjects, onOpenLab }: QcPageProps) {
  const { selectedSlicerDatasetRunId, selectedQcDatasetRunId } = usePipelineContext();
  const runId = selectedQcDatasetRunId ?? selectedSlicerDatasetRunId;

  const [clips, setClips] = useState<Clip[]>([]);
  const [summary, setSummary] = useState<Record<string, unknown>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!runId) return;
    setLoading(true);
    setError(null);
    fetchDatasetSlicerResults(runId)
      .then((r) => {
        setClips(r.candidate_review_manifest as Clip[]);
        setSummary(r.candidate_review_summary);
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
    return <WorkspaceStatePanel title="No project selected" message="Select a project before QC." />;
  }
  if (!runId) {
    return <WorkspaceStatePanel title="No slicer run selected" message="Run the Slicer stage first, then return to QC." />;
  }

  const needsReview = clips.filter((c) => c.needs_review).length;
  const clean = clips.length - needsReview;
  const totalDuration = clips.reduce((s, c) => s + (Number(c.duration_sec) || 0), 0);

  return (
    <section className="step-page pipeline-page qc-page">
      <div className="legacy-demo-banner" style={{ background: "#7c3aed", color: "#fff", padding: "8px 16px", borderRadius: 6, marginBottom: 16, fontSize: 13 }}>
        <strong>Demo QC</strong> — candidate clips from dataset run. Decisions are local only.
      </div>

      <div className="panel pipeline-hero" style={{ marginBottom: 16 }}>
        <p className="eyebrow">Dataset Candidate QC</p>
        <h2>Slicer run: <code style={{ fontSize: "0.85em" }}>{runId}</code></h2>
        <div style={{ display: "flex", gap: 24, marginTop: 12 }}>
          <span><strong>{clips.length}</strong> candidates</span>
          <span><strong>{needsReview}</strong> needs review</span>
          <span><strong>{clean}</strong> clean</span>
          <span><strong>{totalDuration.toFixed(1)}s</strong> total</span>
        </div>
        {summary.total_duration_sec !== undefined && (
          <p style={{ marginTop: 8, color: "#888", fontSize: 13 }}>
            Summary: {JSON.stringify(summary).slice(0, 200)}
          </p>
        )}
        <div style={{ marginTop: 16 }}>
          <button
            className="action-button"
            onClick={() => onOpenLab({ source: "qc", datasetRunId: runId, qcRunId: null, bucketFilter: "all", sort: "source-order", keepThreshold: null, rejectThreshold: null, preset: null })}
          >
            Open Clip Lab →
          </button>
        </div>
      </div>

      {loading && <p>Loading candidates…</p>}
      {error && <p style={{ color: "red" }}>{error}</p>}

      {!loading && clips.length > 0 && (
        <div className="panel" style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
            <thead>
              <tr style={{ textAlign: "left", borderBottom: "1px solid #333" }}>
                <th style={{ padding: "6px 10px" }}>ID</th>
                <th style={{ padding: "6px 10px" }}>Duration</th>
                <th style={{ padding: "6px 10px" }}>Status</th>
                <th style={{ padding: "6px 10px" }}>Reasons</th>
                <th style={{ padding: "6px 10px" }}>Training text</th>
                <th style={{ padding: "6px 10px" }}>Audio</th>
              </tr>
            </thead>
            <tbody>
              {clips.map((clip) => (
                <tr key={String(clip.id)} style={{ borderBottom: "1px solid #222" }}>
                  <td style={{ padding: "6px 10px", fontFamily: "monospace", fontSize: 11 }}>{String(clip.id).slice(0, 20)}</td>
                  <td style={{ padding: "6px 10px" }}>{Number(clip.duration_sec).toFixed(2)}s</td>
                  <td style={{ padding: "6px 10px" }}>
                    <span style={{ color: clip.needs_review ? "#f59e0b" : "#22c55e" }}>
                      {clip.needs_review ? "⚠ Review" : "✓ Clean"}
                    </span>
                  </td>
                  <td style={{ padding: "6px 10px", fontSize: 11, color: "#aaa" }}>
                    {((clip.review_reason_codes as string[]) ?? []).join(", ") || "—"}
                  </td>
                  <td style={{ padding: "6px 10px", maxWidth: 300, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {String(clip.training_text ?? "")}
                  </td>
                  <td style={{ padding: "6px 10px" }}>
                    <audio
                      src={`${API_BASE}/media/dataset-runs/${runId}/candidate-review/${String(clip.id)}.wav`}
                      controls
                      style={{ height: 28 }}
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
