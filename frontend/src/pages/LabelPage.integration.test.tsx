import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { ClipLabItemRef, DatasetClipLabClipRow, DatasetClipLabView, Project } from "../types";
import LabelPage from "./LabelPage";

const pipelineState = {
  selectedLabDatasetRunId: "dataset-run-1" as string | null,
  selectedSlicerDatasetRunId: null as string | null,
  selectedQcDatasetRunId: null as string | null,
};

const clipLabStore = vi.hoisted(() => ({
  view: null as DatasetClipLabView | null,
}));

const clipLabFetchControls = vi.hoisted(() => ({
  staleRun1Fetch: null as Promise<DatasetClipLabView> | null,
}));

vi.mock("../pipeline/PipelineContext", () => ({
  usePipelineContext: () => ({
    selectedLabDatasetRunId: pipelineState.selectedLabDatasetRunId,
    selectLabDatasetRun: (id: string | null) => {
      pipelineState.selectedLabDatasetRunId = id;
    },
    selectedSlicerDatasetRunId: pipelineState.selectedSlicerDatasetRunId,
    selectSlicerDatasetRun: (id: string | null) => {
      pipelineState.selectedSlicerDatasetRunId = id;
    },
    selectedQcDatasetRunId: pipelineState.selectedQcDatasetRunId,
    selectQcDatasetRun: (id: string | null) => {
      pipelineState.selectedQcDatasetRunId = id;
    },
  }),
}));

vi.mock("../WaveformPane", () => ({
  default: () => <div data-testid="waveform-stub" />,
}));

vi.mock("../api", () => ({
  ApiError: class ApiError extends Error {
    status: number;
    url: string;
    constructor(message: string, status: number, url: string) {
      super(message);
      this.status = status;
      this.url = url;
    }
  },
  API_BASE: "http://127.0.0.1:8010",
  appendClipEdlOperation: vi.fn(),
  appendDatasetAudioOperation: vi.fn(),
  fetchClipLabItem: vi.fn(),
  fetchDatasetClipLab: vi.fn(async () => clipLabStore.view),
  fetchDatasetQc: vi.fn(async () => ({
    run_id: "dataset-run-1",
    ready: true,
    missing_artifacts: [],
    invalid_artifacts: [],
    defaults: { transcript_match_threshold: 0.8, speaker_check_threshold: 0.8 },
    finalized: false,
    clips: [],
  })),
  fetchDatasetSlicerResults: vi.fn(),
  fetchProjectDatasetRuns: vi.fn(),
  fetchProjectExports: vi.fn(async () => []),
  fetchProjectRecordings: vi.fn(),
  fetchProjectReferenceAssets: vi.fn(async () => []),
  markDatasetClipAsReferenceCandidate: vi.fn(),
  mergeWithNextClip: vi.fn(),
  patchDatasetClipLabClip: vi.fn(),
  redoClip: vi.fn(),
  redoDatasetAudioOperation: vi.fn(),
  resolveApiUrl: vi.fn((url: string) => url),
  runClipLabModel: vi.fn(),
  saveCurrentSliceAsReference: vi.fn(),
  saveClipState: vi.fn(),
  setActiveVariant: vi.fn(),
  splitClip: vi.fn(),
  undoClip: vi.fn(),
  undoDatasetAudioOperation: vi.fn(),
  fetchDatasetClipLabWaveformPeaks: vi.fn(async () => ({
    revision_key: "source-sha",
    bins: 960,
    peaks: [0.1, 0.2],
    duration_sec: 1.5,
    sample_rate_hz: 16000,
  })),
}));

import {
  fetchDatasetClipLab,
  fetchDatasetSlicerResults,
  fetchProjectDatasetRuns,
  fetchProjectRecordings,
  patchDatasetClipLabClip,
} from "../api";

const activeProject: Project = {
  id: "project-1",
  name: "Test Project",
  created_at: "2026-01-01T00:00:00.000Z",
  updated_at: "2026-01-01T00:00:00.000Z",
  export_status: null,
};

const datasetRun = {
  id: "dataset-run-1",
  project_id: "project-1",
  pipeline_version: "pretraining_rfc_v1",
  artifact_root: "dataset-runs/project-1/dataset-run-1",
  stage: "dataset_qc",
  status: "completed" as const,
  config_hash: null,
  input_summary: {},
  output_summary: {},
  reason_codes: [],
  created_at: "2026-01-01T00:00:00.000Z",
  started_at: "2026-01-01T00:00:01.000Z",
  completed_at: "2026-01-01T00:10:00.000Z",
  artifacts: [
    {
      id: "dataset-run-1:candidate_review_manifest_json",
      kind: "candidate_review_manifest_json",
      path: "artifacts/candidate_review_manifest.json",
      summary: {},
      reason_codes: [],
    },
  ],
};

const datasetRun2 = {
  ...datasetRun,
  id: "dataset-run-2",
  artifact_root: "dataset-runs/project-1/dataset-run-2",
  artifacts: [
    {
      id: "dataset-run-2:candidate_review_manifest_json",
      kind: "candidate_review_manifest_json",
      path: "artifacts/candidate_review_manifest.json",
      summary: {},
      reason_codes: [],
    },
  ],
};

const recording = {
  id: "recording-1",
  batch_id: "project-1",
  display_name: "podcast.wav",
  parent_recording_id: null,
  sample_rate: 16000,
  num_channels: 1,
  num_samples: 160000,
  processing_recipe: null,
  duration_seconds: 10,
};

function makeClipLabRow(overrides: Partial<DatasetClipLabClipRow> = {}): DatasetClipLabClipRow {
  return {
    clip_id: "candidate_review_clip_000003",
    clip_version: 0,
    review_status: "accepted",
    transcript: "Maybe we should try again.",
    original_transcript: "Maybe we should try again.",
    transcript_override: null,
    reviewer_tags: ["good energy"],
    pipeline_findings: [{ code: "clip_contains_oov", label: "clip contains OOV" }],
    content_hash: "hash-accepted",
    accepted_content_hash: "hash-accepted",
    accepted_at: "2026-01-01T00:00:00Z",
    acceptance_stale: false,
    transcript_match: 0.91,
    speaker_check: 0.88,
    sample_rate_hz: 16000,
    effective_audio_kind: "candidate_original",
    effective_audio_revision_key: "source-sha",
    source_audio_sha256: "source-sha",
    audio_revision_hash: null,
    rendered_audio_sha256: null,
    audio_url: "/media/dataset-runs/dataset-run-1/clip-lab/candidate_review_clip_000003/audio/source-sha.wav",
    waveform_peaks_url:
      "/api/dataset-runs/dataset-run-1/clips/candidate_review_clip_000003/waveform-peaks/source-sha",
    current_duration_sec: 1.5,
    audio_edit_op_count: 0,
    audio_edit_ops: [],
    can_undo_audio: false,
    can_redo_audio: false,
    render_status: "ready",
    ...overrides,
  };
}

function makeClipLabView(overrides: Partial<DatasetClipLabView> = {}): DatasetClipLabView {
  return {
    run_id: "dataset-run-1",
    candidate_manifest_sha256: "manifest-sha-1",
    stale_state: false,
    stale_reason: null,
    invalid_state: false,
    invalid_state_reason: null,
    saved_state_clip_count: 1,
    qc_available: true,
    qc_error: null,
    clips: [makeClipLabRow()],
    ...overrides,
  };
}

function Harness({
  datasetRunId = "dataset-run-1",
  remountKey,
}: {
  datasetRunId?: string;
  remountKey?: string;
}) {
  pipelineState.selectedLabDatasetRunId = datasetRunId;

  const [activeClipItem, setActiveClipItem] = useState<ClipLabItemRef | null>({
    id: "candidate_review_clip_000003",
  });

  return (
    <LabelPage
      key={remountKey}
      activeProject={activeProject}
      activeClipItem={activeClipItem}
      projectLoadStatus="ready"
      projectLoadError={null}
      onActiveClipItemChange={setActiveClipItem}
      onRetryProjects={() => {}}
      onHeaderActionsChange={() => {}}
    />
  );
}

afterEach(() => {
  cleanup();
  pipelineState.selectedLabDatasetRunId = "dataset-run-1";
  pipelineState.selectedSlicerDatasetRunId = null;
  pipelineState.selectedQcDatasetRunId = null;
});

beforeEach(() => {
  clipLabFetchControls.staleRun1Fetch = null;
  clipLabStore.view = makeClipLabView();

  vi.mocked(fetchProjectDatasetRuns).mockResolvedValue([datasetRun, datasetRun2]);
  vi.mocked(fetchProjectRecordings).mockResolvedValue([recording]);
  vi.mocked(fetchDatasetSlicerResults).mockImplementation(async (runId) => ({
    run_id: runId,
    safe_cutpoint_summary: {},
    candidate_review_summary: {},
    candidate_review_manifest: [
      {
        id: "candidate_review_clip_000003",
        source_audio_id: "source_0",
        source_start_sample: 0,
        source_end_sample: 60800,
        sample_rate: 16000,
        duration_sec: 3.8,
        training_text: "Maybe we should try again.",
        review_reason_codes: ["clip_contains_oov"],
        needs_review: true,
      },
    ],
    candidate_review_rejected: [],
  }));
  vi.mocked(fetchDatasetClipLab).mockImplementation(async (runId) => {
    if (runId === "dataset-run-1" && clipLabFetchControls.staleRun1Fetch) {
      return clipLabFetchControls.staleRun1Fetch;
    }
    if (runId === "dataset-run-2") {
      return makeClipLabView({
        run_id: "dataset-run-2",
        candidate_manifest_sha256: "manifest-sha-2",
        clips: [makeClipLabRow({ reviewer_tags: ["run-two-tag"] })],
      });
    }
    return clipLabStore.view ?? makeClipLabView();
  });
  vi.mocked(patchDatasetClipLabClip).mockImplementation(async (_runId, clipId, payload) => {
    const current = clipLabStore.view?.clips.find((clip) => clip.clip_id === clipId);
    if (!current) {
      throw new Error("missing clip");
    }
    if (payload.expected_clip_version !== current.clip_version) {
      const { ApiError } = await import("../api");
      throw new ApiError("stale clip", 409, "/clip-lab");
    }
    const updated = makeClipLabRow({
      clip_id: clipId,
      clip_version: current.clip_version + 1,
      reviewer_tags: payload.reviewer_tags ?? current.reviewer_tags,
      review_status: payload.review_status ?? current.review_status,
      transcript_override:
        payload.transcript_override !== undefined
          ? payload.transcript_override
          : current.transcript_override,
    });
    clipLabStore.view = {
      ...clipLabStore.view!,
      clips: clipLabStore.view!.clips.map((clip) => (clip.clip_id === clipId ? updated : clip)),
    };
    return updated;
  });
});

describe("LabelPage dataset Clip Lab integration", () => {
  it("loads machine findings and reviewer tags, then PATCHes a new tag through the editor", async () => {
    render(<Harness />);

    await waitFor(() => {
      expect(screen.getAllByText("clip contains OOV").length).toBeGreaterThanOrEqual(1);
      expect(screen.getAllByText("good energy").length).toBeGreaterThanOrEqual(1);
    });

    const input = screen.getByPlaceholderText("Add tag (press Enter)") as HTMLInputElement;
    fireEvent.change(input, { target: { value: "mouth noise" } });
    fireEvent.keyDown(input, { key: "Enter" });

    await waitFor(() => {
      expect(patchDatasetClipLabClip).toHaveBeenCalled();
    });

    const lastCall = vi.mocked(patchDatasetClipLabClip).mock.calls.at(-1);
    expect(lastCall?.[2]).toMatchObject({
      expected_clip_version: 0,
      reviewer_tags: ["good energy", "mouth noise"],
    });

    await waitFor(() => {
      expect(screen.getAllByText("mouth noise").length).toBeGreaterThanOrEqual(1);
    });
  });

  it("disables tag editing when clip lab state is stale", async () => {
    clipLabStore.view = makeClipLabView({
      stale_state: true,
      stale_reason: "candidate manifest changed",
    });

    render(<Harness />);

    await waitFor(() => {
      expect(screen.getByText(/older candidate manifest/i)).toBeTruthy();
    });

    expect(screen.queryByPlaceholderText("Add tag (press Enter)")).toBeNull();
    expect(screen.getAllByText("clip contains OOV").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("good energy").length).toBeGreaterThanOrEqual(1);
  });

  it("does not call PATCH when Save is clicked without changes", async () => {
    render(<Harness />);

    await waitFor(() => {
      expect(screen.getAllByText("good energy").length).toBeGreaterThanOrEqual(1);
    });

    vi.mocked(patchDatasetClipLabClip).mockClear();

    fireEvent.click(screen.getByRole("button", { name: /^save$/i }));

    await waitFor(() => {
      expect(screen.getByText("Saved clip state.")).toBeTruthy();
    });

    expect(patchDatasetClipLabClip).not.toHaveBeenCalled();
    expect(screen.getAllByText("good energy").length).toBeGreaterThanOrEqual(1);
  });

  it("ignores a stale clip lab response after switching dataset runs", async () => {
    let resolveStaleRun1: (view: DatasetClipLabView) => void;
    clipLabFetchControls.staleRun1Fetch = new Promise<DatasetClipLabView>((resolve) => {
      resolveStaleRun1 = resolve;
    });

    const { rerender } = render(<Harness datasetRunId="dataset-run-1" remountKey="run-1" />);

    await waitFor(() => {
      expect(screen.queryByPlaceholderText("Add tag (press Enter)")).toBeNull();
    });

    clipLabFetchControls.staleRun1Fetch = null;
    rerender(<Harness datasetRunId="dataset-run-2" remountKey="run-2" />);

    await waitFor(() => {
      expect(screen.getAllByText("run-two-tag").length).toBeGreaterThanOrEqual(1);
    });
    expect(screen.getByPlaceholderText("Add tag (press Enter)")).toBeTruthy();

    resolveStaleRun1!(
      makeClipLabView({
        clips: [makeClipLabRow({ reviewer_tags: ["run-one-stale-tag"] })],
      }),
    );

    await waitFor(() => {
      expect(screen.queryByText("run-one-stale-tag")).toBeNull();
    });
    expect(screen.getAllByText("run-two-tag").length).toBeGreaterThanOrEqual(1);
  });

  it("resolves dataset audio and peaks URLs without double prefixes", async () => {
    const { resolveApiUrl } = await import("../api");
    vi.mocked(resolveApiUrl).mockClear();

    render(<Harness />);

    await waitFor(() => {
      expect(screen.getAllByText("good energy").length).toBeGreaterThanOrEqual(1);
    });

    const row = makeClipLabRow();
    await waitFor(() => {
      expect(resolveApiUrl).toHaveBeenCalledWith(row.audio_url);
      expect(resolveApiUrl).toHaveBeenCalledWith(row.waveform_peaks_url);
    });

    for (const call of vi.mocked(resolveApiUrl).mock.calls) {
      const input = String(call[0]);
      expect(input).not.toMatch(/^https?:\/\//);
      expect(input).not.toMatch(/https?:\/\/.+https?:\/\//);
    }
  });

  it("treats stale clip lab state as read-only across editor and inspector", async () => {
    clipLabStore.view = makeClipLabView({
      stale_state: true,
      stale_reason: "candidate manifest changed",
    });

    render(<Harness />);

    await waitFor(() => {
      expect(screen.getAllByText("good energy").length).toBeGreaterThanOrEqual(1);
    });

    expect(screen.getByText(/clip lab state is stale/i)).toBeTruthy();
    expect(screen.getByText(/status controls are unavailable/i)).toBeTruthy();
    const saveButton = screen.getByRole("button", { name: /^save$/i }) as HTMLButtonElement;
    expect(saveButton.disabled).toBe(true);
    expect((screen.getByRole("button", { name: /accept & next/i }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByRole("button", { name: /reject & next/i }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByRole("button", { name: /insert silence/i }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByPlaceholderText("Transcript text") as HTMLTextAreaElement).disabled).toBe(true);

    vi.mocked(patchDatasetClipLabClip).mockClear();
    fireEvent.click(screen.getByRole("button", { name: /^save$/i }));
    expect(patchDatasetClipLabClip).not.toHaveBeenCalled();
  });

  it("treats invalid clip lab state as read-only across editor and inspector", async () => {
    clipLabStore.view = makeClipLabView({
      invalid_state: true,
      invalid_state_reason: "clip_lab_state.json failed validation",
    });

    render(<Harness />);

    await waitFor(() => {
      expect(screen.getAllByText("good energy").length).toBeGreaterThanOrEqual(1);
    });

    expect(screen.getAllByText(/clip lab state is invalid/i).length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText(/status controls are unavailable/i)).toBeTruthy();
    expect((screen.getByRole("button", { name: /^save$/i }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByRole("button", { name: /accept & next/i }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByPlaceholderText("Transcript text") as HTMLTextAreaElement).disabled).toBe(true);

    vi.mocked(patchDatasetClipLabClip).mockClear();
    fireEvent.click(screen.getByRole("button", { name: /^save$/i }));
    expect(patchDatasetClipLabClip).not.toHaveBeenCalled();
  });
});
