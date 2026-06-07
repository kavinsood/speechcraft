import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import SlicerPage from "./SlicerPage";

const pipelineState = {
  selectedSlicerDatasetRunId: null as string | null,
};

vi.mock("../pipeline/PipelineContext", () => ({
  usePipelineContext: () => ({
    selectedSlicerDatasetRunId: pipelineState.selectedSlicerDatasetRunId,
    selectSlicerDatasetRun: (id: string | null) => {
      pipelineState.selectedSlicerDatasetRunId = id;
    },
  }),
}));
vi.mock("../api", () => ({
  ApiError: class ApiError extends Error {},
  buildCandidateReviewAudioUrl: vi.fn((runId: string, clipId: string) => `/media/dataset-runs/${runId}/candidate-review/${clipId}.wav`),
  fetchDatasetRunLog: vi.fn(),
  fetchDatasetSlicerResults: vi.fn(),
  fetchProjectDatasetRuns: vi.fn(),
  refreshDatasetRun: vi.fn(),
  rerunDatasetSlicer: vi.fn(),
}));

import { buildCandidateReviewAudioUrl, fetchDatasetRunLog, fetchDatasetSlicerResults, fetchProjectDatasetRuns, rerunDatasetSlicer } from "../api";

const alignmentArtifacts = [
  { id: "a1", kind: "aligned_words_jsonl", path: "artifacts/aligned_words.jsonl", summary: {}, reason_codes: [] },
  { id: "a2", kind: "alignment_qc_by_buffer_json", path: "artifacts/alignment_qc_by_buffer.json", summary: {}, reason_codes: [] },
];

const alignedRun = {
  id: "run-1",
  project_id: "project-1",
  pipeline_version: "pretraining_rfc_v1",
  artifact_root: "dataset-runs/project-1/run-1",
  stage: "alignment_qc",
  status: "completed" as const,
  config_hash: null,
  input_summary: {},
  output_summary: {},
  reason_codes: [],
  created_at: "2026-06-05T00:00:00Z",
  started_at: null,
  completed_at: "2026-06-05T00:00:01Z",
  artifacts: alignmentArtifacts,
};

const slicedRun = {
  ...alignedRun,
  stage: "candidate_clips",
  artifacts: [
    ...alignmentArtifacts,
    { id: "a3", kind: "candidate_review_manifest_json", path: "artifacts/candidate_review_manifest.json", summary: {}, reason_codes: [] },
  ],
};

function renderPage() {
  return render(
    <SlicerPage
      activeProject={{ id: "project-1", name: "Project", created_at: "", updated_at: "", export_status: null }}
      projectLoadStatus="ready"
      projectLoadError={null}
      onRetryProjects={() => {}}
      onOpenQc={() => {}}
    />,
  );
}

beforeEach(() => {
  pipelineState.selectedSlicerDatasetRunId = "run-1";
  vi.clearAllMocks();
  vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([alignedRun]);
  vi.mocked(fetchDatasetRunLog).mockResolvedValue({ run_id: alignedRun.id, path: "logs/dataset_worker.log", text: "slicer done", truncated: false });
  vi.mocked(fetchDatasetSlicerResults).mockResolvedValue({
    run_id: alignedRun.id,
    safe_cutpoint_summary: {},
    candidate_review_summary: {},
    candidate_review_manifest: [],
    candidate_review_rejected: [],
  });
  vi.mocked(rerunDatasetSlicer).mockResolvedValue({ ...alignedRun, status: "running", stage: "safe_cutpoints" });
});
afterEach(cleanup);

describe("SlicerPage", () => {
  it("shows alignment-ready state before candidate artifacts exist", async () => {
    renderPage();
    expect(await screen.findByText("Alignment is ready. SafeCutPoints and candidate clips have not been generated yet.")).not.toBeNull();
    expect(screen.getByRole("button", { name: "Generate candidate clips" })).not.toBeNull();
  });

  it("calls slicer-rerun when Generate is clicked", async () => {
    renderPage();
    fireEvent.click(await screen.findByRole("button", { name: "Generate candidate clips" }));
    await waitFor(() => expect(rerunDatasetSlicer).toHaveBeenCalledTimes(1));
    expect(vi.mocked(rerunDatasetSlicer).mock.calls[0][1]).toMatchObject({ cutpoint_min_gap_ms: 80, candidate_target_clip_sec: 8 });
  });

  it("shows existing candidate clips and Regenerate label when artifacts exist", async () => {
    pipelineState.selectedSlicerDatasetRunId = "run-1";
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([slicedRun]);
    vi.mocked(fetchDatasetSlicerResults).mockResolvedValue({
      run_id: slicedRun.id,
      safe_cutpoint_summary: { accepted_cutpoints: 12, rejected_cutpoint_candidates: 8, acceptance_rate: 0.6, rejection_reason_counts: { usable_gap_too_short: 8 } },
      candidate_review_summary: { candidate_review_clips: 2, total_duration_sec: 15, clips_needing_review: 1, rejected_spans: 1 },
      candidate_review_manifest: [{ id: "clip-1", training_text: "hello world", duration_sec: 7.5, needs_review: false }],
      candidate_review_rejected: [],
    });
    const view = renderPage();
    expect(await screen.findByText("Existing candidate clips found for this run.")).not.toBeNull();
    expect(screen.getByRole("button", { name: "Regenerate SafeCutPoints + assembly" })).not.toBeNull();
    expect(screen.getByText("usable gap too short")).not.toBeNull();
    expect(view.container.querySelector("audio")).not.toBeNull();
    expect(buildCandidateReviewAudioUrl).toHaveBeenCalledWith("run-1", "clip-1");
    expect(screen.getByText("hello world")).not.toBeNull();
  });

  it("does not auto-select the first run when none is selected", async () => {
    pipelineState.selectedSlicerDatasetRunId = null;
    renderPage();
    await screen.findByText("No dataset runs. Complete Processing first.");
    expect(fetchDatasetSlicerResults).not.toHaveBeenCalled();
    expect(screen.getByText("Select a dataset run from the sidebar or use Open Slicer on a completed Processing run.")).not.toBeNull();
  });
});
