import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import ReferencePage from "./ReferencePage";

vi.mock("../api", () => ({
  createReferenceRun: vi.fn(),
  fetchProjectReferenceAssets: vi.fn(),
  fetchProjectReferenceRuns: vi.fn(),
  fetchProjectSourceRecordings: vi.fn(),
  fetchReferenceAsset: vi.fn(),
  fetchReferenceRun: vi.fn(),
  fetchReferenceRunCandidates: vi.fn(),
  promoteReferenceCandidate: vi.fn(),
  rerankReferenceRunCandidates: vi.fn(),
  buildReferenceCandidateAudioUrl: vi.fn((runId: string, candidateId: string) => `/candidate/${runId}/${candidateId}.wav`),
  buildReferenceVariantAudioUrl: vi.fn((variantId: string) => `/reference/${variantId}.wav`),
  ApiError: class ApiError extends Error {
    status: number;
    url: string;

    constructor(message: string, status: number, url: string) {
      super(message);
      this.status = status;
      this.url = url;
    }
  },
}));

import {
  fetchProjectReferenceAssets,
  fetchProjectReferenceRuns,
  fetchProjectSourceRecordings,
  fetchReferenceAsset,
  fetchReferenceRun,
  fetchReferenceRunCandidates,
  rerankReferenceRunCandidates,
} from "../api";

const mockedFetchProjectSourceRecordings = vi.mocked(fetchProjectSourceRecordings);
const mockedFetchProjectReferenceRuns = vi.mocked(fetchProjectReferenceRuns);
const mockedFetchProjectReferenceAssets = vi.mocked(fetchProjectReferenceAssets);
const mockedFetchReferenceAsset = vi.mocked(fetchReferenceAsset);
const mockedFetchReferenceRun = vi.mocked(fetchReferenceRun);
const mockedFetchReferenceRunCandidates = vi.mocked(fetchReferenceRunCandidates);
const mockedRerankReferenceRunCandidates = vi.mocked(rerankReferenceRunCandidates);

function makeRecording(id: string) {
  return {
    id,
    batch_id: "project-1",
    parent_recording_id: null,
    sample_rate: 48000,
    num_channels: 1,
    num_samples: 480000,
    processing_recipe: null,
    duration_seconds: 10,
  };
}

function makeRun(id: string, status: "queued" | "running" | "completed" | "failed") {
  return {
    id,
    project_id: "project-1",
    status,
    mode: "both",
    config: { recording_ids: ["src-1"] },
    candidate_count: status === "completed" ? 1 : 0,
    error_message: status === "failed" ? "boom" : null,
    created_at: "2026-03-21T00:00:00Z",
    started_at: "2026-03-21T00:00:01Z",
    completed_at: status === "completed" || status === "failed" ? "2026-03-21T00:00:02Z" : null,
  };
}

function makeCandidate(runId: string, candidateId: string) {
  return {
    candidate_id: candidateId,
    run_id: runId,
    source_media_kind: "source_recording" as const,
    source_recording_id: "src-1",
    source_variant_id: null,
    source_start_seconds: 1.0,
    source_end_seconds: 5.5,
    duration_seconds: 4.5,
    transcript_text: "Reference candidate line",
    speaker_name: "speaker_a",
    language: "en",
    risk_flags: [],
    default_scores: { both: 0.92, overall: 0.92, zero_shot: 0.91, finetune: 0.93 },
  };
}

function renderReferencePage() {
  return render(
    <ReferencePage
      activeProject={{ id: "project-1", name: "Project 1", created_at: "", updated_at: "", export_status: null }}
      projectLoadStatus="ready"
      projectLoadError={null}
      onRetryProjects={() => {}}
    />,
  );
}

async function flushMicrotasks() {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
}

beforeEach(() => {
  vi.clearAllMocks();
  window.history.replaceState({}, "", "/reference");
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe("ReferencePage", () => {
  it("stops polling when a selected run fails", async () => {
    vi.useFakeTimers();
    mockedFetchProjectSourceRecordings.mockResolvedValue([makeRecording("src-1")]);
    mockedFetchProjectReferenceRuns.mockResolvedValue([makeRun("run-1", "queued")]);
    mockedFetchProjectReferenceAssets.mockResolvedValue([]);
    mockedFetchReferenceRun.mockResolvedValue(makeRun("run-1", "failed"));
    mockedFetchReferenceRunCandidates.mockResolvedValue([]);

    renderReferencePage();
    await flushMicrotasks();

    expect(screen.getAllByText("boom").length).toBeGreaterThan(0);
    expect(mockedFetchReferenceRun).toHaveBeenCalledTimes(1);

    await act(async () => {
      vi.advanceTimersByTime(5000);
    });

    expect(mockedFetchReferenceRun).toHaveBeenCalledTimes(1);
  });

  it("does not preselect all recordings when a project has multiple sources", async () => {
    mockedFetchProjectSourceRecordings.mockResolvedValue([makeRecording("src-1"), makeRecording("src-2")]);
    mockedFetchProjectReferenceRuns.mockResolvedValue([]);
    mockedFetchProjectReferenceAssets.mockResolvedValue([]);

    renderReferencePage();
    await screen.findByRole("button", { name: "Start Candidate Run" });

    const sourceOne = screen.getByLabelText(/src-1/i) as HTMLInputElement;
    const sourceTwo = screen.getByLabelText(/src-2/i) as HTMLInputElement;
    const startButton = screen.getByRole("button", { name: "Start Candidate Run" }) as HTMLButtonElement;

    expect(sourceOne.checked).toBe(false);
    expect(sourceTwo.checked).toBe(false);
    expect(startButton.disabled).toBe(true);
  });

  it("reranks candidates when a like anchor is toggled", async () => {
    mockedFetchProjectSourceRecordings.mockResolvedValue([makeRecording("src-1")]);
    mockedFetchProjectReferenceRuns.mockResolvedValue([makeRun("run-current", "completed")]);
    mockedFetchProjectReferenceAssets.mockResolvedValue([]);
    mockedFetchReferenceRun.mockResolvedValue(makeRun("run-current", "completed"));
    mockedFetchReferenceRunCandidates.mockResolvedValue([
      makeCandidate("run-current", "cand-a"),
      makeCandidate("run-current", "cand-b"),
    ]);
    mockedRerankReferenceRunCandidates.mockResolvedValue({
      run_id: "run-current",
      mode: "both",
      positive_candidate_ids: ["cand-b"],
      negative_candidate_ids: [],
      candidates: [
        {
          ...makeCandidate("run-current", "cand-b"),
          mode: "both",
          base_score: 0.92,
          intent_score: 0.75,
          rerank_score: 1.67,
        },
        {
          ...makeCandidate("run-current", "cand-a"),
          mode: "both",
          base_score: 0.92,
          intent_score: -0.1,
          rerank_score: 0.82,
        },
      ],
    });

    renderReferencePage();
    await screen.findAllByRole("button", { name: "Like" });

    fireEvent.click(screen.getAllByRole("button", { name: "Like" })[1]);

    await waitFor(() => {
      expect(mockedRerankReferenceRunCandidates).toHaveBeenCalledWith("run-current", {
        positive_candidate_ids: ["cand-b"],
        negative_candidate_ids: [],
        mode: "both",
      });
    });

    await screen.findByText("1.670");
    expect(screen.getByText(/Likes:/)).toBeTruthy();
  });

  it("shows promoted-state across runs when candidate identity already exists in the library", async () => {
    mockedFetchProjectSourceRecordings.mockResolvedValue([makeRecording("src-1")]);
    mockedFetchProjectReferenceRuns.mockResolvedValue([makeRun("run-current", "completed")]);
    mockedFetchProjectReferenceAssets.mockResolvedValue([
      {
        id: "asset-1",
        project_id: "project-1",
        name: "Saved earlier",
        status: "active" as const,
        transcript_text: "Reference candidate line",
        speaker_name: "speaker_a",
        language: "en",
        mood_label: null,
        active_variant_id: "reference-variant-1",
        created_from_run_id: "run-older",
        created_from_candidate_id: "cand-shared",
        source_slice_id: null,
        source_audio_variant_id: null,
        source_edit_commit_id: null,
        created_at: "2026-03-21T00:00:00Z",
        updated_at: "2026-03-21T00:00:00Z",
        active_variant: {
          id: "reference-variant-1",
          reference_asset_id: "asset-1",
          source_kind: "source_recording" as const,
          source_recording_id: "src-1",
          source_slice_id: null,
          source_audio_variant_id: null,
          source_reference_variant_id: null,
          source_start_seconds: 1.0,
          source_end_seconds: 5.5,
          is_original: true,
          generator_model: "reference-picker",
          sample_rate: 48000,
          num_samples: 216000,
          deleted: false,
          created_at: "2026-03-21T00:00:00Z",
        },
      },
    ]);
    mockedFetchReferenceAsset.mockResolvedValue({
      id: "asset-1",
      project_id: "project-1",
      name: "Saved earlier",
      status: "active" as const,
      transcript_text: "Reference candidate line",
      speaker_name: "speaker_a",
      language: "en",
      mood_label: null,
      active_variant_id: "reference-variant-1",
      created_from_run_id: "run-older",
      created_from_candidate_id: "cand-shared",
      source_slice_id: null,
      source_audio_variant_id: null,
      source_edit_commit_id: null,
      created_at: "2026-03-21T00:00:00Z",
      updated_at: "2026-03-21T00:00:00Z",
      active_variant: {
        id: "reference-variant-1",
        reference_asset_id: "asset-1",
        source_kind: "source_recording" as const,
        source_recording_id: "src-1",
        source_slice_id: null,
        source_audio_variant_id: null,
        source_reference_variant_id: null,
        source_start_seconds: 1.0,
        source_end_seconds: 5.5,
        is_original: true,
        generator_model: "reference-picker",
        sample_rate: 48000,
        num_samples: 216000,
        deleted: false,
        created_at: "2026-03-21T00:00:00Z",
      },
      notes: null,
      favorite_rank: null,
      model_metadata: null,
      variants: [],
    });
    mockedFetchReferenceRun.mockResolvedValue(makeRun("run-current", "completed"));
    mockedFetchReferenceRunCandidates.mockResolvedValue([makeCandidate("run-current", "cand-shared")]);

    renderReferencePage();

    await screen.findByText(/Already saved as/i);
    expect(screen.getByRole("button", { name: "Open Existing" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Promote Again" })).toBeTruthy();

    await waitFor(() => {
      expect(mockedFetchReferenceRunCandidates).toHaveBeenCalledWith("run-current", { limit: 100 });
    });
  });
});
