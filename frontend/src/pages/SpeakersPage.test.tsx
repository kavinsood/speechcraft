import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import SpeakersPage from "./SpeakersPage";

const pipelineState = {
  selectedSpeakersRunId: null as string | null,
};

vi.mock("../pipeline/PipelineContext", () => ({
  usePipelineContext: () => ({
    selectedSpeakersRunId: pipelineState.selectedSpeakersRunId,
    selectSpeakersRun: (id: string | null) => {
      pipelineState.selectedSpeakersRunId = id;
    },
  }),
}));

vi.mock("../api", () => ({
  ApiError: class ApiError extends Error {},
  API_BASE: "http://127.0.0.1:8010",
  buildSpeakerSampleAudioUrl: vi.fn((runId: string, sampleId: string) => `http://127.0.0.1:8010/media/dataset-runs/${runId}/speaker-samples/${sampleId}.wav`),
  createProjectDatasetRun: vi.fn(),
  fetchDatasetRunLog: vi.fn(),
  fetchDatasetSpeakerResults: vi.fn(),
  fetchProjectDatasetRuns: vi.fn(),
  fetchProjectSourceRecordings: vi.fn(),
  refreshDatasetRun: vi.fn(),
  saveDatasetSpeakerSelection: vi.fn(),
  startDatasetRun: vi.fn(),
}));

import {
  createProjectDatasetRun,
  fetchDatasetRunLog,
  fetchDatasetSpeakerResults,
  fetchProjectDatasetRuns,
  fetchProjectSourceRecordings,
  refreshDatasetRun,
  saveDatasetSpeakerSelection,
  startDatasetRun,
} from "../api";

const recording = {
  id: "recording-1",
  batch_id: "project-1",
  display_name: "two-hosts.wav",
  parent_recording_id: null,
  sample_rate: 44100,
  num_channels: 1,
  num_samples: 441000,
  processing_recipe: null,
  duration_seconds: 2131.5,
};

const diarizationRun = {
  id: "dataset-run-1",
  project_id: "project-1",
  pipeline_version: "pretraining_rfc_v1",
  artifact_root: "dataset-runs/project-1/dataset-run-1",
  stage: "diarization",
  status: "completed" as const,
  config_hash: null,
  input_summary: { single_speaker: false },
  output_summary: {},
  reason_codes: [],
  created_at: "2026-06-05T00:00:00Z",
  started_at: "2026-06-05T00:00:01Z",
  completed_at: "2026-06-05T00:00:02Z",
  artifacts: [],
};

function renderPage(onOpenProcessing = vi.fn(), onOpenProcessingWithRun = vi.fn()) {
  return render(
    <SpeakersPage
      activeProject={{ id: "project-1", name: "Project 1", created_at: "", updated_at: "", export_status: null }}
      projectLoadStatus="ready"
      projectLoadError={null}
      onRetryProjects={() => {}}
      onOpenProcessing={onOpenProcessing}
      onOpenProcessingWithRun={onOpenProcessingWithRun}
    />,
  );
}

beforeEach(() => {
  pipelineState.selectedSpeakersRunId = null;
  vi.clearAllMocks();
  vi.mocked(fetchProjectSourceRecordings).mockResolvedValue([recording]);
  vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([]);
  vi.mocked(fetchDatasetRunLog).mockResolvedValue({ run_id: diarizationRun.id, path: "logs/dataset_worker.log", text: "speaker detection", truncated: false });
  vi.mocked(fetchDatasetSpeakerResults).mockResolvedValue({
    run_id: diarizationRun.id,
    speaker_regions_summary: {},
    speaker_samples_manifest: [],
    speaker_selection: null,
  });
  vi.mocked(refreshDatasetRun).mockResolvedValue(diarizationRun);
  vi.mocked(createProjectDatasetRun).mockResolvedValue({ ...diarizationRun, status: "pending" });
  vi.mocked(startDatasetRun).mockResolvedValue({ ...diarizationRun, status: "running" });
  vi.mocked(saveDatasetSpeakerSelection).mockResolvedValue({
    mode: "diarization",
    selected: true,
    target_speaker_id: "speaker_0",
    source: "user",
    available_speaker_ids: ["speaker_0", "speaker_1"],
  });
});

afterEach(() => {
  cleanup();
});

describe("SpeakersPage", () => {
  it("lets single-speaker projects continue without running diarization", async () => {
    const onOpenProcessing = vi.fn();
    renderPage(onOpenProcessing);

    fireEvent.click(await screen.findByRole("button", { name: "Continue to Processing" }));
    expect(onOpenProcessing).toHaveBeenCalledTimes(1);
  });

  it("creates a diarization run for multi-speaker detection", async () => {
    renderPage();

    fireEvent.click(await screen.findByRole("button", { name: "Run speaker detection" }));
    await waitFor(() => expect(createProjectDatasetRun).toHaveBeenCalledTimes(1));
    expect(vi.mocked(createProjectDatasetRun).mock.calls[0][1]).toMatchObject({
      source_recording_ids: ["recording-1"],
      single_speaker: false,
      stop_after: "diarization",
    });
    await waitFor(() => expect(startDatasetRun).toHaveBeenCalledTimes(1));
  });

  it("keeps a created pending diarization run visible and startable when initial start fails", async () => {
    vi.mocked(createProjectDatasetRun).mockResolvedValue({ ...diarizationRun, status: "pending" });
    vi.mocked(startDatasetRun).mockRejectedValueOnce(
      new TypeError("NetworkError when attempting to fetch resource."),
    );
    vi.mocked(fetchDatasetSpeakerResults).mockResolvedValue({
      run_id: diarizationRun.id,
      speaker_regions_summary: {},
      speaker_samples_manifest: [],
      speaker_selection: null,
    });

    renderPage();

    fireEvent.click(await screen.findByRole("button", { name: "Run speaker detection" }));

    expect(
      await screen.findByText(
        "Backend API is offline at http://127.0.0.1:8010. Restart make dev-backend, then refresh.",
      ),
    ).not.toBeNull();
    expect(screen.getByRole("button", { name: "Start selected pending run" })).not.toBeNull();
    expect(screen.getByRole("button", { name: /dataset-run-1/i })).not.toBeNull();
  });

  it("loads speaker cards, saves selection, and hands off to Processing", async () => {
    pipelineState.selectedSpeakersRunId = diarizationRun.id;
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([diarizationRun]);
    vi.mocked(fetchDatasetSpeakerResults)
      .mockResolvedValueOnce({
        run_id: diarizationRun.id,
        speaker_regions_summary: {
          per_speaker: {
            speaker_0: { duration_sec: 120, segment_count: 12 },
            speaker_1: { duration_sec: 45, segment_count: 6 },
          },
        },
        speaker_samples_manifest: [
          {
            sample_id: "speaker_0_00",
            speaker_id: "speaker_0",
            source_audio_id: "recording-1",
            audio_path: "artifacts/speaker_samples/speaker_0_00.wav",
            start_sample: 0,
            end_sample: 96000,
            duration_sec: 6,
          },
        ],
        speaker_selection: null,
      })
      .mockResolvedValueOnce({
        run_id: diarizationRun.id,
        speaker_regions_summary: {
          per_speaker: {
            speaker_0: { duration_sec: 120, segment_count: 12 },
            speaker_1: { duration_sec: 45, segment_count: 6 },
          },
        },
        speaker_samples_manifest: [
          {
            sample_id: "speaker_0_00",
            speaker_id: "speaker_0",
            source_audio_id: "recording-1",
            audio_path: "artifacts/speaker_samples/speaker_0_00.wav",
            start_sample: 0,
            end_sample: 96000,
            duration_sec: 6,
          },
        ],
        speaker_selection: {
          mode: "diarization",
          selected: true,
          target_speaker_id: "speaker_0",
          source: "user",
          available_speaker_ids: ["speaker_0", "speaker_1"],
        },
      });
    const onOpenProcessingWithRun = vi.fn();
    const { container } = renderPage(vi.fn(), onOpenProcessingWithRun);

    await screen.findByText("speaker_0");
    const sampleAudio = container.querySelector("audio");
    expect(sampleAudio?.getAttribute("src")).toContain("/media/dataset-runs/dataset-run-1/speaker-samples/speaker_0_00.wav");
    fireEvent.click((await screen.findAllByRole("button", { name: "Choose this speaker" }))[0]!);
    await waitFor(() => expect(saveDatasetSpeakerSelection).toHaveBeenCalledWith(diarizationRun.id, "speaker_0"));

    const continueButtons = await screen.findAllByRole("button", { name: "Continue to Processing" });
    fireEvent.click(continueButtons[continueButtons.length - 1]!);
    expect(onOpenProcessingWithRun).toHaveBeenCalledWith(diarizationRun.id);
  });
});
