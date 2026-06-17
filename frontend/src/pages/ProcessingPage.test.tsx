import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import ProcessingPage from "./ProcessingPage";

const pipelineState = {
  selectedSpeakersRunId: null as string | null,
  selectedProcessingRunId: null as string | null,
  selectedSlicerDatasetRunId: null as string | null,
};

vi.mock("../pipeline/PipelineContext", () => ({
  usePipelineContext: () => ({
    selectedSpeakersRunId: pipelineState.selectedSpeakersRunId,
    selectSpeakersRun: (id: string | null) => {
      pipelineState.selectedSpeakersRunId = id;
    },
    selectedProcessingRunId: pipelineState.selectedProcessingRunId,
    selectProcessingRun: (id: string | null) => {
      pipelineState.selectedProcessingRunId = id;
    },
    selectedSlicerDatasetRunId: pipelineState.selectedSlicerDatasetRunId,
    selectSlicerDatasetRun: (id: string | null) => {
      pipelineState.selectedSlicerDatasetRunId = id;
    },
  }),
}));

vi.mock("../api", () => ({
  ApiError: class ApiError extends Error {},
  API_BASE: "http://127.0.0.1:8010",
  createProjectDatasetRun: vi.fn(),
  fetchDatasetSpeakerResults: vi.fn(),
  fetchDatasetPreflight: vi.fn(),
  fetchDatasetRunLog: vi.fn(),
  fetchProjectDatasetRuns: vi.fn(),
  fetchProjectSourceRecordings: vi.fn(),
  refreshDatasetRun: vi.fn(),
  resumeDatasetRunProcessing: vi.fn(),
  startDatasetRun: vi.fn(),
}));

import {
  createProjectDatasetRun,
  fetchDatasetSpeakerResults,
  fetchDatasetPreflight,
  fetchDatasetRunLog,
  fetchProjectDatasetRuns,
  fetchProjectSourceRecordings,
  refreshDatasetRun,
  resumeDatasetRunProcessing,
  startDatasetRun,
} from "../api";

const recording = {
  id: "recording-1",
  batch_id: "project-1",
  display_name: "poki-podcast.wav",
  parent_recording_id: null,
  sample_rate: 48000,
  num_channels: 1,
  num_samples: 480000,
  processing_recipe: null,
  duration_seconds: 3671,
};

const run = {
  id: "dataset-run-1",
  project_id: "project-1",
  pipeline_version: "pretraining_rfc_v1",
  artifact_root: "dataset-runs/project-1/dataset-run-1",
  stage: "vad",
  status: "running" as const,
  config_hash: null,
  input_summary: {},
  output_summary: {},
  reason_codes: [],
  created_at: "2026-06-05T00:00:00Z",
  started_at: "2026-06-05T00:00:01Z",
  completed_at: null,
  artifacts: [],
};

function runWithStatus(status: "pending" | "running" | "completed" | "failed", stage = "vad") {
  return {
    ...run,
    stage,
    status,
    completed_at: status === "completed" || status === "failed" ? "2026-06-05T00:00:02Z" : null,
  };
}

function renderPage(onOpenSlicerWithRun = vi.fn(), onOpenSpeakers = vi.fn()) {
  return render(
    <ProcessingPage
      activeProject={{ id: "project-1", name: "Project 1", created_at: "", updated_at: "", export_status: null }}
      projectLoadStatus="ready"
      projectLoadError={null}
      onRetryProjects={() => {}}
      onOpenSpeakers={onOpenSpeakers}
      onOpenSlicerWithRun={onOpenSlicerWithRun}
    />,
  );
}

beforeEach(() => {
  pipelineState.selectedSpeakersRunId = null;
  pipelineState.selectedProcessingRunId = null;
  pipelineState.selectedSlicerDatasetRunId = null;
  vi.clearAllMocks();
  vi.mocked(fetchProjectSourceRecordings).mockResolvedValue([recording]);
  vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([]);
  vi.mocked(fetchDatasetSpeakerResults).mockResolvedValue({
    run_id: "dataset-run-1",
    speaker_regions_summary: {},
    speaker_samples_manifest: [],
    speaker_selection: null,
  });
  vi.mocked(fetchDatasetPreflight).mockResolvedValue({ ok: true });
  vi.mocked(fetchDatasetRunLog).mockResolvedValue({ run_id: run.id, path: "logs/dataset_worker.log", text: "worker started", truncated: false });
  vi.mocked(refreshDatasetRun).mockResolvedValue(run);
  vi.mocked(createProjectDatasetRun).mockResolvedValue({ ...run, status: "pending" });
  vi.mocked(resumeDatasetRunProcessing).mockResolvedValue(run);
  vi.mocked(startDatasetRun).mockResolvedValue(run);
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

function artifact(id: string, kind: string) {
  return { id, kind, path: `artifacts/${kind}.json`, summary: {}, reason_codes: [] };
}

function runAtStage(status: "pending" | "running" | "completed" | "failed", stage: string, artifactCount: number) {
  return {
    ...runWithStatus(status, stage),
    artifacts: Array.from({ length: artifactCount }, (_, index) => artifact(`artifact-${index}`, "run_status_json")),
  };
}

describe("ProcessingPage", () => {
  it("auto-selects loaded sources and launches a single-speaker dataset run", async () => {
    renderPage();
    const launch = await screen.findByRole("button", { name: "Create and start single-speaker run" }) as HTMLButtonElement;
    expect(launch.disabled).toBe(false);
    expect(screen.getByText("poki-podcast.wav")).not.toBeNull();
    expect(screen.getByText(/48000 Hz · 1 ch · 1h 1m 11s/)).not.toBeNull();
    expect(screen.getByText("1 source WAV(s) selected for single-speaker processing.")).not.toBeNull();

    fireEvent.click(launch);

    await waitFor(() => expect(createProjectDatasetRun).toHaveBeenCalledTimes(1));
    expect(vi.mocked(createProjectDatasetRun).mock.calls[0][1]).toMatchObject({
      source_recording_ids: ["recording-1"],
      single_speaker: true,
      stop_after: "alignment_qc",
    });
    await waitFor(() => expect(startDatasetRun).toHaveBeenCalledWith("dataset-run-1"));
  });

  it("uses a generic source label for old uploads without original filenames", async () => {
    vi.mocked(fetchProjectSourceRecordings).mockResolvedValue([{ ...recording, display_name: "source-f0f597db0c0f40abb38e59dfec26928d.wav" }]);
    renderPage();

    expect(await screen.findByText("Source audio 1")).not.toBeNull();
    expect(screen.queryByText("source-f0f597db0c0f40abb38e59dfec26928d.wav")).toBeNull();
  });

  it("blocks launch when no source is selected", async () => {
    renderPage();
    const launch = await screen.findByRole("button", { name: "Create and start single-speaker run" }) as HTMLButtonElement;
    expect(launch.disabled).toBe(false);

    fireEvent.click(screen.getByRole("checkbox", { name: /recording-1/i }));
    expect(launch.disabled).toBe(true);
    expect(screen.getByText("Select at least one source WAV in the sidebar.")).not.toBeNull();
  });

  it("blocks launch when dataset worker preflight fails", async () => {
    vi.mocked(fetchDatasetPreflight).mockResolvedValue({
      ok: false,
      error: "ASR model snapshot is incomplete: missing model.bin",
      asr_model: {
        ok: false,
        model: "medium.en",
        snapshot_check: { ok: false, missing_files: ["model.bin"] },
      },
    });
    renderPage();

    await screen.findByText("ASR model unavailable");
    expect((screen.getByRole("button", { name: "Create and start single-speaker run" }) as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getAllByText("ASR model snapshot is incomplete: missing model.bin").length).toBeGreaterThan(0);
  });

  it("loads run history without waiting for slow preflight", async () => {
    let finishPreflight: (value: { ok: boolean }) => void = () => {};
    vi.mocked(fetchDatasetPreflight).mockImplementation(
      () => new Promise((resolve) => {
        finishPreflight = resolve;
      }),
    );
    renderPage();

    expect(await screen.findByText("No dataset runs yet.")).not.toBeNull();
    expect(screen.getByText("Checking ASR model small.en")).not.toBeNull();
    expect((screen.getByRole("button", { name: "Create and start single-speaker run" }) as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByText("Waiting for dataset worker preflight.")).not.toBeNull();
    finishPreflight({ ok: true });
    await waitFor(() => expect((screen.getByRole("button", { name: "Create and start single-speaker run" }) as HTMLButtonElement).disabled).toBe(false));
  });

  it("keeps the main page usable when preflight cannot reach the backend", async () => {
    vi.mocked(fetchDatasetPreflight).mockRejectedValue(new Error("Backend API is unreachable at http://127.0.0.1:8010. Restart make dev-backend, then refresh."));
    renderPage();

    expect(await screen.findByText("ASR model unavailable")).not.toBeNull();
    expect(
      screen.getAllByText("Backend API is unreachable at http://127.0.0.1:8010. Restart make dev-backend, then refresh.").length,
    ).toBeGreaterThan(0);
    expect(screen.getByRole("button", { name: "Create and start single-speaker run" })).not.toBeNull();
  });

  it("requests preflight for the selected ASR model and updates when the model changes", async () => {
    renderPage();

    await screen.findByRole("button", { name: "Create and start single-speaker run" });
    expect(fetchDatasetPreflight).toHaveBeenCalledWith({
      asrModel: "small.en",
      asrDevice: "cuda",
      asrComputeType: "float16",
    });

    fireEvent.change(screen.getByLabelText("Whisper model"), { target: { value: "medium.en" } });

    await waitFor(() =>
      expect(fetchDatasetPreflight).toHaveBeenLastCalledWith({
        asrModel: "medium.en",
        asrDevice: "cuda",
        asrComputeType: "float16",
      }),
    );
  });

  it("keeps processing parameter controls available while hiding VAD controls", async () => {
    const { container } = renderPage();

    await screen.findByRole("button", { name: "Create and start single-speaker run" });
    const details = Array.from(container.querySelectorAll("details")).find((node) => node.textContent?.includes("Pipeline parameters"));
    expect(details?.open).toBe(false);

    fireEvent.click(screen.getByText("Pipeline parameters"));
    expect(details?.open).toBe(true);
    expect(screen.queryByLabelText("VAD threshold")).toBeNull();
    expect(screen.getByLabelText("Processing buffer maximum")).not.toBeNull();
  });

  it("shows selected run terminal output", async () => {
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([run]);
    renderPage();

    expect(await screen.findByText("worker started")).not.toBeNull();
    expect(screen.getByText(/running · 0 indexed artifact/)).not.toBeNull();
  });

  it("shows a local warning when run status loads but terminal log fails", async () => {
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([run]);
    vi.mocked(fetchDatasetRunLog).mockRejectedValue(new Error("log file missing"));
    renderPage();

    expect(await screen.findByText("log file missing")).not.toBeNull();
    expect(screen.getByText(/running · 0 indexed artifact/)).not.toBeNull();
  });

  it("lets the user start an existing selected pending run", async () => {
    const pending = runWithStatus("pending");
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([pending]);
    vi.mocked(refreshDatasetRun).mockResolvedValue(pending);
    renderPage();

    const startPending = await screen.findByRole("button", { name: "Start selected pending run" });
    expect((startPending as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(startPending);

    await waitFor(() => expect(startDatasetRun).toHaveBeenCalledWith(pending.id));
  });

  it("keeps a created run visible and startable when initial start fails", async () => {
    const pending = runWithStatus("pending");
    vi.mocked(createProjectDatasetRun).mockResolvedValue(pending);
    vi.mocked(startDatasetRun).mockRejectedValueOnce(new Error("worker launch failed"));
    vi.mocked(refreshDatasetRun).mockResolvedValue(pending);
    renderPage();

    const launch = await screen.findByRole("button", { name: "Create and start single-speaker run" }) as HTMLButtonElement;
    await waitFor(() => expect(launch.disabled).toBe(false));
    fireEvent.click(launch);

    expect(await screen.findByText("worker launch failed")).not.toBeNull();
    expect(screen.getByRole("button", { name: "Start selected pending run" })).not.toBeNull();
    expect(screen.getByText(/pending · 0 artifacts/)).not.toBeNull();
  });

  it("blocks new runs while one is running", async () => {
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([runWithStatus("running")]);
    renderPage();

    expect((await screen.findByRole("button", { name: "Create and start single-speaker run" }) as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByText("A dataset worker is already active.")).not.toBeNull();
  });

  it("allows a new run after the previous run completed", async () => {
    const completed = runWithStatus("completed", "alignment_qc");
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([completed]);
    vi.mocked(refreshDatasetRun).mockResolvedValue(completed);
    renderPage();

    expect((await screen.findByRole("button", { name: "Create and start single-speaker run" }) as HTMLButtonElement).disabled).toBe(false);
  });

  it("shows Open Slicer CTA when alignment QC completed", async () => {
    const completed = runWithStatus("completed", "alignment_qc");
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([completed]);
    vi.mocked(refreshDatasetRun).mockResolvedValue(completed);
    const onOpenSlicerWithRun = vi.fn();
    renderPage(onOpenSlicerWithRun);

    const handoff = await screen.findByRole("button", { name: "Open Slicer with this run" });
    fireEvent.click(handoff);
    expect(onOpenSlicerWithRun).toHaveBeenCalledWith(completed.id);
  });

  it("does not change slicer selection when clicking a processing run", async () => {
    const completed = runWithStatus("completed", "alignment_qc");
    const other = { ...runWithStatus("completed", "mfa"), id: "dataset-run-2" };
    pipelineState.selectedSlicerDatasetRunId = "slicer-run-1";
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([completed, other]);
    vi.mocked(refreshDatasetRun).mockImplementation(async (runId) => [completed, other].find((entry) => entry.id === runId) ?? completed);
    renderPage();

    await screen.findByRole("button", { name: "Open Slicer with this run" });
    fireEvent.click(screen.getByRole("button", { name: /mfa/i }));
    expect(pipelineState.selectedProcessingRunId).toBe("dataset-run-2");
    expect(pipelineState.selectedSlicerDatasetRunId).toBe("slicer-run-1");
  });

  it("polls refresh every 2s while a run is active and updates artifact counts", async () => {
    const pollCallbacks: Array<() => void> = [];
    const setIntervalSpy = vi.spyOn(window, "setInterval").mockImplementation((handler, delay) => {
      pollCallbacks.push(handler as () => void);
      return pollCallbacks.length as unknown as ReturnType<typeof setInterval>;
    });
    const runningAsr = runAtStage("running", "asr", 5);
    const runningMfa = runAtStage("running", "mfa", 12);
    const completed = runAtStage("completed", "alignment_qc", 20);
    pipelineState.selectedProcessingRunId = runningAsr.id;
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([runningAsr]);
    vi.mocked(refreshDatasetRun).mockResolvedValue(runningAsr);
    renderPage();

    await screen.findByText(/running · 5 indexed artifact/);
    expect(setIntervalSpy).toHaveBeenCalledWith(expect.any(Function), 2000);
    expect(vi.mocked(refreshDatasetRun).mock.calls.length).toBeGreaterThan(0);

    vi.mocked(refreshDatasetRun).mockResolvedValue(runningMfa);
    await act(async () => {
      pollCallbacks[pollCallbacks.length - 1]?.();
    });
    await screen.findByText(/running · 12 indexed artifact/);

    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([completed]);
    vi.mocked(refreshDatasetRun).mockResolvedValue(completed);
    fireEvent.click(screen.getByRole("button", { name: "Refresh all" }));
    await screen.findByText(/completed · 20 indexed artifact/);
    expect(screen.queryByRole("button", { name: "Open Slicer with this run" })).not.toBeNull();
  });

  it("backs off polling and shows a warning after refresh failures", async () => {
    const pollCallbacks: Array<() => void> = [];
    const setIntervalSpy = vi.spyOn(window, "setInterval").mockImplementation((handler, delay) => {
      pollCallbacks.push(handler as () => void);
      return pollCallbacks.length as unknown as ReturnType<typeof setInterval>;
    });
    const runningAsr = runAtStage("running", "asr", 5);
    pipelineState.selectedProcessingRunId = runningAsr.id;
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([runningAsr]);
    vi.mocked(refreshDatasetRun).mockResolvedValue(runningAsr);
    renderPage();

    await screen.findByText(/running · 5 indexed artifact/);
    expect(setIntervalSpy).toHaveBeenLastCalledWith(expect.any(Function), 2000);

    vi.mocked(refreshDatasetRun).mockRejectedValueOnce(
      new Error("Backend API is unreachable at http://127.0.0.1:8010. Restart make dev-backend, then refresh."),
    );
    await act(async () => {
      pollCallbacks[pollCallbacks.length - 1]?.();
    });
    expect(
      await screen.findByText("Backend API is unreachable at http://127.0.0.1:8010. Restart make dev-backend, then refresh."),
    ).not.toBeNull();
    await waitFor(() =>
      expect(setIntervalSpy.mock.calls.some(([, delay]) => delay === 5000)).toBe(true),
    );
  });

  it("shows Open Slicer CTA only after alignment_qc completion, not during running", async () => {
    const running = runAtStage("running", "mfa", 8);
    pipelineState.selectedProcessingRunId = running.id;
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([running]);
    vi.mocked(refreshDatasetRun).mockResolvedValue(running);
    renderPage();

    await screen.findByText(/running · 8 indexed artifact/);
    expect(screen.queryByRole("button", { name: "Open Slicer with this run" })).toBeNull();

    const completed = runAtStage("completed", "alignment_qc", 20);
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([completed]);
    vi.mocked(refreshDatasetRun).mockResolvedValue(completed);
    fireEvent.click(screen.getByRole("button", { name: "Refresh all" }));
    expect(await screen.findByRole("button", { name: "Open Slicer with this run" })).not.toBeNull();
  });

  it("follows the active running run in the terminal when another run is selected", async () => {
    const completed = { ...runAtStage("completed", "alignment_qc", 2), id: "dataset-run-old" };
    const active = { ...runAtStage("running", "asr", 6), id: "dataset-run-active" };
    pipelineState.selectedProcessingRunId = completed.id;
    pipelineState.selectedSlicerDatasetRunId = "slicer-run-1";
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([completed, active]);
    vi.mocked(refreshDatasetRun).mockImplementation(async (runId) => (
      runId === active.id ? active : completed
    ));
    vi.mocked(fetchDatasetRunLog).mockImplementation(async (runId) => ({
      run_id: runId,
      path: "logs/dataset_worker.log",
      text: runId === active.id ? "asr transcribing" : "old run log",
      truncated: false,
    }));
    renderPage();

    expect(await screen.findByText("asr transcribing")).not.toBeNull();
    expect(screen.getByText(/running · 6 indexed artifact/)).not.toBeNull();
    expect(screen.getByText(/Terminal follows the active run/i)).not.toBeNull();
    expect(screen.getByText("dataset-run-active", { selector: "strong" })).not.toBeNull();
  });

  it("sends a multi-speaker run without selection back to Speakers", async () => {
    const diarizationRun = {
      ...runWithStatus("completed", "diarization"),
      input_summary: { single_speaker: false },
    };
    pipelineState.selectedProcessingRunId = diarizationRun.id;
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([diarizationRun]);
    vi.mocked(refreshDatasetRun).mockResolvedValue(diarizationRun);
    vi.mocked(fetchDatasetSpeakerResults).mockResolvedValue({
      run_id: diarizationRun.id,
      speaker_regions_summary: {},
      speaker_samples_manifest: [],
      speaker_selection: { mode: "diarization", selected: false, target_speaker_id: null, source: "user", available_speaker_ids: ["speaker_0"] },
    });
    const onOpenSpeakers = vi.fn();
    renderPage(vi.fn(), onOpenSpeakers);

    expect(await screen.findByText(/need a chosen target speaker/i)).not.toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Return to Speakers" }));
    expect(onOpenSpeakers).toHaveBeenCalledTimes(1);
  });

  it("continues a selected multi-speaker run through resume-processing", async () => {
    const diarizationRun = {
      ...runWithStatus("completed", "diarization"),
      input_summary: { single_speaker: false },
    };
    pipelineState.selectedProcessingRunId = diarizationRun.id;
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([diarizationRun]);
    vi.mocked(refreshDatasetRun).mockResolvedValue(diarizationRun);
    vi.mocked(fetchDatasetSpeakerResults).mockResolvedValue({
      run_id: diarizationRun.id,
      speaker_regions_summary: {},
      speaker_samples_manifest: [],
      speaker_selection: { mode: "diarization", selected: true, target_speaker_id: "speaker_0", source: "user", available_speaker_ids: ["speaker_0"] },
    });
    renderPage();

    const continueButton = await screen.findByRole("button", { name: "Continue processing selected speaker" });
    fireEvent.click(continueButton);
    await waitFor(() =>
      expect(resumeDatasetRunProcessing).toHaveBeenCalledWith(diarizationRun.id, "alignment_qc"),
    );
  });

  it("adopts the selected speakers run when Processing opens without its own run selection", async () => {
    const diarizationRun = {
      ...runWithStatus("completed", "diarization"),
      input_summary: { single_speaker: false },
    };
    pipelineState.selectedSpeakersRunId = diarizationRun.id;
    pipelineState.selectedProcessingRunId = null;
    vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([diarizationRun]);
    vi.mocked(refreshDatasetRun).mockResolvedValue(diarizationRun);
    vi.mocked(fetchDatasetSpeakerResults).mockResolvedValue({
      run_id: diarizationRun.id,
      speaker_regions_summary: {},
      speaker_samples_manifest: [],
      speaker_selection: { mode: "diarization", selected: true, target_speaker_id: "speaker_0", source: "user", available_speaker_ids: ["speaker_0"] },
    });
    renderPage();

    expect(await screen.findByRole("button", { name: "Continue processing selected speaker" })).not.toBeNull();
    expect(pipelineState.selectedProcessingRunId).toBe(diarizationRun.id);
  });
});
