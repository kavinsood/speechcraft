import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import QcPage from "./QcPage";

const pipelineState = {
  selectedQcDatasetRunId: "run-1" as string | null,
  selectedSlicerDatasetRunId: null as string | null,
};

vi.mock("../pipeline/PipelineContext", () => ({
  usePipelineContext: () => ({
    selectedQcDatasetRunId: pipelineState.selectedQcDatasetRunId,
    selectedSlicerDatasetRunId: pipelineState.selectedSlicerDatasetRunId,
    selectQcDatasetRun: (id: string | null) => {
      pipelineState.selectedQcDatasetRunId = id;
    },
  }),
}));

vi.mock("../api", () => ({
  ApiError: class ApiError extends Error {},
  fetchDatasetQc: vi.fn(),
  fetchProjectDatasetRuns: vi.fn(),
  finalizeDatasetQc: vi.fn(),
  generateDatasetQcScores: vi.fn(),
  refreshDatasetRun: vi.fn(),
  resolveApiUrl: (pathOrUrl: string) => pathOrUrl,
}));

import { fetchDatasetQc, fetchProjectDatasetRuns, finalizeDatasetQc, generateDatasetQcScores, refreshDatasetRun } from "../api";

const project = {
  id: "project-1",
  name: "Project",
  created_at: "",
  updated_at: "",
};

const run = {
  id: "run-1",
  project_id: "project-1",
  pipeline_version: "pretraining_rfc_v1",
  artifact_root: "dataset-runs/project-1/run-1",
  stage: "speaker_purity",
  status: "completed" as const,
  config_hash: null,
  input_summary: {},
  output_summary: {},
  reason_codes: [],
  created_at: "2026-06-18T00:00:00Z",
  started_at: null,
  completed_at: "2026-06-18T00:00:01Z",
  artifacts: [
    { id: "a1", kind: "candidate_review_manifest_json", path: "artifacts/candidate_review_manifest.json", summary: {}, reason_codes: [] },
    { id: "a2", kind: "transcript_qc_json", path: "artifacts/transcript_qc.json", summary: {}, reason_codes: [] },
    { id: "a3", kind: "speaker_purity_json", path: "artifacts/speaker_purity.json", summary: {}, reason_codes: [] },
  ],
};

const readyPayload = {
  run_id: "run-1",
  ready: true,
  missing_artifacts: [],
  invalid_artifacts: [],
  defaults: {
    transcript_match_threshold: 85,
    speaker_check_threshold: 70,
  },
  finalized: false,
  finalized_thresholds: null,
  clips: [
    {
      clip_id: "clip-1",
      audio_path: "artifacts/candidate_review/clip-1.wav",
      audio_url: "/media/dataset-runs/run-1/candidate-review/clip-1.wav",
      duration_sec: 1,
      training_text: "steady pass clip",
      alignment_text: null,
      transcript_match: 92,
      speaker_check: 88,
      transcript_reason_codes: [],
      speaker_reason_codes: [],
      candidate_reason_codes: [],
      qc_reason_codes: [],
      weak_transcript_spans: [],
      weak_speaker_spans: [],
      manual_override: null,
    },
    {
      clip_id: "clip-2",
      audio_path: "artifacts/candidate_review/clip-2.wav",
      audio_url: "/media/dataset-runs/run-1/candidate-review/clip-2.wav",
      duration_sec: 1.5,
      training_text: "borderline reject clip",
      alignment_text: null,
      transcript_match: 60,
      speaker_check: 90,
      transcript_reason_codes: ["low_transcript_match"],
      speaker_reason_codes: [],
      candidate_reason_codes: [],
      qc_reason_codes: [],
      weak_transcript_spans: [],
      weak_speaker_spans: [],
      manual_override: null,
    },
    {
      clip_id: "clip-3",
      audio_path: "artifacts/candidate_review/clip-3.wav",
      audio_url: "/media/dataset-runs/run-1/candidate-review/clip-3.wav",
      duration_sec: 1.2,
      training_text: "balanced edge keep",
      alignment_text: null,
      transcript_match: 86,
      speaker_check: 72,
      transcript_reason_codes: [],
      speaker_reason_codes: [],
      candidate_reason_codes: [],
      qc_reason_codes: [],
      weak_transcript_spans: [],
      weak_speaker_spans: [],
      manual_override: null,
    },
    {
      clip_id: "clip-4",
      audio_path: "artifacts/candidate_review/clip-4.wav",
      audio_url: "/media/dataset-runs/run-1/candidate-review/clip-4.wav",
      duration_sec: 1.3,
      training_text: "speaker edge keep",
      alignment_text: null,
      transcript_match: 90,
      speaker_check: 71,
      transcript_reason_codes: [],
      speaker_reason_codes: [],
      candidate_reason_codes: [],
      qc_reason_codes: [],
      weak_transcript_spans: [],
      weak_speaker_spans: [],
      manual_override: null,
    },
    {
      clip_id: "clip-5",
      audio_path: "artifacts/candidate_review/clip-5.wav",
      audio_url: "/media/dataset-runs/run-1/candidate-review/clip-5.wav",
      duration_sec: 1.4,
      training_text: "transcript edge keep",
      alignment_text: null,
      transcript_match: 85,
      speaker_check: 95,
      transcript_reason_codes: [],
      speaker_reason_codes: [],
      candidate_reason_codes: [],
      qc_reason_codes: [],
      weak_transcript_spans: [],
      weak_speaker_spans: [],
      manual_override: null,
    },
    {
      clip_id: "clip-6",
      audio_path: "artifacts/candidate_review/clip-6.wav",
      audio_url: "/media/dataset-runs/run-1/candidate-review/clip-6.wav",
      duration_sec: 1.1,
      training_text: "speaker reject clip",
      alignment_text: null,
      transcript_match: 90,
      speaker_check: 60,
      transcript_reason_codes: [],
      speaker_reason_codes: ["low_speaker_similarity_window"],
      candidate_reason_codes: [],
      qc_reason_codes: [],
      weak_transcript_spans: [],
      weak_speaker_spans: [],
      manual_override: null,
    },
  ],
};

function renderPage() {
  return render(
    <QcPage
      activeProject={project}
      projectLoadStatus="ready"
      projectLoadError={null}
      onRetryProjects={() => {}}
    />,
  );
}

function sectionByHeading(name: string): HTMLElement {
  return screen.getByRole("heading", { name }).closest("section") as HTMLElement;
}

function auditRow(section: HTMLElement, text: string): HTMLElement {
  return within(section).getByText(text).closest(".qc-audit-row") as HTMLElement;
}

function firstAuditRowByText(text: string): HTMLElement {
  return screen.getAllByText(text)[0].closest(".qc-audit-row") as HTMLElement;
}

beforeEach(() => {
  pipelineState.selectedQcDatasetRunId = "run-1";
  pipelineState.selectedSlicerDatasetRunId = null;
  vi.clearAllMocks();
  vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([run]);
  vi.mocked(fetchDatasetQc).mockResolvedValue(readyPayload);
  vi.mocked(refreshDatasetRun).mockResolvedValue(run);
  vi.mocked(generateDatasetQcScores).mockResolvedValue(run);
  vi.mocked(finalizeDatasetQc).mockResolvedValue({
    run_id: "run-1",
    dataset_qc_path: "artifacts/dataset_qc.json",
    summary: {
      accepted_count: 4,
      rejected_count: 2,
      accepted_duration_sec: 4.9,
      rejected_duration_sec: 2.6,
    },
  });
});

afterEach(() => {
  vi.useRealTimers();
  cleanup();
});

describe("QcPage", () => {
  it("applies local overrides in the live summary and sends them on finalize", async () => {
    renderPage();

    expect(await screen.findByText("4 clips · 5s")).not.toBeNull();
    expect(screen.getByText("2 clips · 3s")).not.toBeNull();

    const bestRejectedSection = sectionByHeading("Best rejected");
    fireEvent.click(within(auditRow(bestRejectedSection, "borderline reject clip")).getByRole("button", { name: "Keep" }));

    expect(await screen.findByText("5 clips · 6s")).not.toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "Finalize QC" }));

    await waitFor(() =>
      expect(finalizeDatasetQc).toHaveBeenCalledWith("run-1", {
        thresholds: {
          transcript_match_min: 85,
          speaker_check_min: 70,
        },
        manual_overrides: [
          {
            clip_id: "clip-2",
            override: "force_keep",
          },
        ],
      }),
    );
  });

  it("shows missing artifact guidance when QC is not ready", async () => {
    vi.mocked(fetchDatasetQc).mockResolvedValueOnce({
      ...readyPayload,
      ready: false,
      clips: [],
      missing_artifacts: ["artifacts/transcript_qc.json", "artifacts/speaker_purity.json"],
    });

    renderPage();

    expect(await screen.findByText("QC is not ready for this run yet.")).not.toBeNull();
    expect(screen.getByText("artifacts/transcript_qc.json")).not.toBeNull();
    expect(screen.getByText("artifacts/speaker_purity.json")).not.toBeNull();
    expect(screen.getByRole("button", { name: "Run QC Scores" })).not.toBeNull();
    expect(screen.getByRole("button", { name: "Finalize QC" }).hasAttribute("disabled")).toBe(true);
  });

  it("starts QC score generation for existing runs with missing score artifacts", async () => {
    vi.mocked(fetchDatasetQc)
      .mockResolvedValueOnce({
        ...readyPayload,
        ready: false,
        clips: [],
        missing_artifacts: ["artifacts/transcript_qc.json", "artifacts/speaker_purity.json"],
      })
      .mockResolvedValueOnce(readyPayload);

    renderPage();

    fireEvent.click(await screen.findByRole("button", { name: "Run QC Scores" }));

    await waitFor(() => expect(generateDatasetQcScores).toHaveBeenCalledWith("run-1"));
    expect(await screen.findByText("Running QC Scores for this dataset run.")).not.toBeNull();
  });

  it("does not show payload unavailable before the first QC load settles", () => {
    vi.mocked(fetchDatasetQc).mockImplementation(() => new Promise(() => {}));

    renderPage();

    expect(screen.getByText("Loading QC artifacts…")).not.toBeNull();
    expect(screen.queryByText("QC payload failed to load. Check backend logs or retry.")).toBeNull();
  });

  it("surfaces the actual QC load error message for the selected run", async () => {
    vi.mocked(fetchDatasetQc).mockRejectedValueOnce(new Error("backend exploded"));

    renderPage();

    expect(await screen.findByText("Could not load Dataset QC for run-1: backend exploded")).not.toBeNull();
    expect(screen.getByText("QC payload failed to load. Check backend logs or retry.")).not.toBeNull();
  });

  it("updates audit lists when thresholds move", async () => {
    renderPage();

    expect(await screen.findByRole("heading", { name: "Worst kept" })).not.toBeNull();
    const acceptedSampleSection = sectionByHeading("Accepted sample");
    expect(within(acceptedSampleSection).getByText("transcript edge keep")).not.toBeNull();
    expect(within(acceptedSampleSection).getAllByText("85.00")[0]).not.toBeNull();
    expect(within(acceptedSampleSection).getAllByText("95.00")[0]).not.toBeNull();

    fireEvent.change(screen.getAllByRole("slider")[0], { target: { value: "91" } });
    fireEvent.mouseUp(screen.getAllByRole("slider")[0]);

    await waitFor(() => expect(screen.getByText("1 clips · 1s")).not.toBeNull());
    expect(within(acceptedSampleSection).queryByText("transcript edge keep")).toBeNull();

    const bestRejectedSection = sectionByHeading("Best rejected");
    expect(within(bestRejectedSection).getByText("transcript edge keep")).not.toBeNull();
  });

  it("supports audit tabs and force reject removes a clip from Worst kept", async () => {
    renderPage();

    const worstKeptSection = await screen.findByRole("heading", { name: "Worst kept" });
    const worstKeptContainer = worstKeptSection.closest("section") as HTMLElement;
    expect(within(worstKeptContainer).getByText("transcript edge keep")).not.toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "Speaker risk" }));
    expect(within(worstKeptContainer).getByText("speaker edge keep")).not.toBeNull();

    fireEvent.click(within(auditRow(worstKeptContainer, "transcript edge keep")).getByRole("button", { name: "Reject" }));

    await waitFor(() =>
      expect(within(worstKeptContainer).queryByText("transcript edge keep")).toBeNull(),
    );
  });

  it("clears overrides back to threshold behavior and submits force reject", async () => {
    renderPage();

    expect(await screen.findByRole("heading", { name: "Best rejected" })).not.toBeNull();
    const bestRejectedSection = sectionByHeading("Best rejected");
    fireEvent.click(within(auditRow(bestRejectedSection, "borderline reject clip")).getByRole("button", { name: "Keep" }));
    expect(await screen.findByText("5 clips · 6s")).not.toBeNull();

    fireEvent.click(within(firstAuditRowByText("borderline reject clip")).getByRole("button", { name: "Clear" }));
    expect(await screen.findByText("4 clips · 5s")).not.toBeNull();

    const worstKeptSection = sectionByHeading("Worst kept");
    fireEvent.click(within(auditRow(worstKeptSection, "steady pass clip")).getByRole("button", { name: "Reject" }));

    fireEvent.click(screen.getByRole("button", { name: "Finalize QC" }));

    await waitFor(() =>
      expect(finalizeDatasetQc).toHaveBeenCalledWith("run-1", {
        thresholds: {
          transcript_match_min: 85,
          speaker_check_min: 70,
        },
        manual_overrides: [
          {
            clip_id: "clip-1",
            override: "force_reject",
          },
        ],
      }),
    );
  });
});
