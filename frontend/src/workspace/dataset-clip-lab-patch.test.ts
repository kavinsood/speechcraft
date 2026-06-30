import { describe, expect, it, vi } from "vitest";

import { ApiError } from "../api";
import type { DatasetClipLabClipRow, DatasetClipLabView } from "../types";
import {
  applyDatasetClipLabRow,
  buildReviewerTagSuggestions,
  clipLabPatchQueueKey,
  createClipLabPatchCoordinator,
  filterTagSuggestions,
} from "./dataset-clip-lab-patch";

function makeClipRow(
  clipId: string,
  clipVersion: number,
  reviewerTags: string[] = [],
): DatasetClipLabClipRow {
  return {
    clip_id: clipId,
    clip_version: clipVersion,
    review_status: "unresolved",
    transcript: "hello",
    original_transcript: "hello",
    transcript_override: null,
    reviewer_tags: reviewerTags,
    pipeline_findings: [],
    content_hash: "hash",
    accepted_content_hash: null,
    accepted_at: null,
    acceptance_stale: false,
    transcript_match: null,
    speaker_check: null,
    sample_rate_hz: 16000,
    effective_audio_kind: "candidate_original",
    effective_audio_revision_key: "source-sha",
    source_audio_sha256: "source-sha",
    audio_revision_hash: null,
    rendered_audio_sha256: null,
    audio_url: `/media/dataset-runs/run-a/clip-lab/${clipId}/audio/source-sha.wav`,
    waveform_peaks_url: `/api/dataset-runs/run-a/clips/${clipId}/waveform-peaks/source-sha`,
    current_duration_sec: 1.0,
    audio_edit_op_count: 0,
    audio_edit_ops: [],
    can_undo_audio: false,
    can_redo_audio: false,
    render_status: "ready",
  };
}

function makeView(runId: string, clips: DatasetClipLabClipRow[]): DatasetClipLabView {
  return {
    run_id: runId,
    candidate_manifest_sha256: "manifest-sha",
    stale_state: false,
    stale_reason: null,
    invalid_state: false,
    invalid_state_reason: null,
    saved_state_clip_count: clips.length,
    qc_available: false,
    qc_error: null,
    clips,
  };
}

describe("dataset-clip-lab-patch helpers", () => {
  it("builds run-qualified queue keys", () => {
    expect(clipLabPatchQueueKey("run-a", "clip-1")).toBe("run-a:clip-1");
  });

  it("preserves the first saved display spelling for suggestions", () => {
    expect(buildReviewerTagSuggestions(["Good Energy", "good energy", "Mouth Noise"])).toEqual([
      "Good Energy",
      "Mouth Noise",
    ]);
  });

  it("filters suggestions by focus query and excludes taken labels", () => {
    expect(
      filterTagSuggestions(
        ["Good Energy", "Mouth Noise", "Breathy"],
        "mouth",
        ["Breathy"],
        ["low energy"],
      ),
    ).toEqual(["Mouth Noise"]);
  });

  it("discards row updates from a different run", () => {
    const current = makeView("run-a", [makeClipRow("clip-1", 0)]);
    const updated = makeClipRow("clip-1", 1, ["stale"]);
    expect(applyDatasetClipLabRow(current, "run-b", updated)).toBe(current);
  });
});

describe("createClipLabPatchCoordinator", () => {
  it("serializes rapid tag additions with fresh clip versions and merged tag lists", async () => {
    let view = makeView("run-a", [makeClipRow("clip-1", 0)]);
    const patchCalls: Array<{ clipVersion: number; reviewerTags: string[] | undefined }> = [];

    const coordinator = createClipLabPatchCoordinator({
      getView: () => view,
      patchClip: async (_runId, _clipId, payload) => {
        patchCalls.push({
          clipVersion: payload.expected_clip_version,
          reviewerTags: payload.reviewer_tags ?? undefined,
        });
        const current = view.clips.find((clip) => clip.clip_id === "clip-1");
        if (!current) {
          throw new Error("missing clip");
        }
        const updated = makeClipRow("clip-1", current.clip_version + 1, payload.reviewer_tags ?? []);
        return updated;
      },
      onViewChange: (next) => {
        view = next;
      },
      onRowUpdated: () => {},
      onConflict: async () => {},
    });

    await Promise.all([
      coordinator.patchDatasetClipLab("run-a", "clip-1", (row) => ({
        reviewer_tags: [...row.reviewer_tags, "good energy"],
      })),
      coordinator.patchDatasetClipLab("run-a", "clip-1", (row) => ({
        reviewer_tags: [...row.reviewer_tags, "mouth noise"],
      })),
    ]);

    expect(patchCalls).toEqual([
      { clipVersion: 0, reviewerTags: ["good energy"] },
      { clipVersion: 1, reviewerTags: ["good energy", "mouth noise"] },
    ]);
    expect(view.clips[0]?.reviewer_tags).toEqual(["good energy", "mouth noise"]);
    expect(view.clips[0]?.clip_version).toBe(2);
  });

  it("does not apply late results after the active run changes", async () => {
    let view: DatasetClipLabView | null = makeView("run-a", [makeClipRow("clip-1", 0)]);
    let resolvePatch: ((row: DatasetClipLabClipRow) => void) | null = null;
    const onRowUpdated = vi.fn();

    const coordinator = createClipLabPatchCoordinator({
      getView: () => view,
      patchClip: async () =>
        await new Promise<DatasetClipLabClipRow>((resolve) => {
          resolvePatch = resolve;
        }),
      onViewChange: (next) => {
        view = next;
      },
      onRowUpdated,
      onConflict: async () => {},
    });

    const pending = coordinator.patchDatasetClipLab("run-a", "clip-1", () => ({
      reviewer_tags: ["run-a-only"],
    }));

    await Promise.resolve();
    view = makeView("run-b", [makeClipRow("clip-1", 0)]);
    resolvePatch?.(makeClipRow("clip-1", 1, ["run-a-only"]));
    await pending;

    expect(view.run_id).toBe("run-b");
    expect(view.clips[0]?.reviewer_tags).toEqual([]);
    expect(onRowUpdated).not.toHaveBeenCalled();
  });

  it("does not commit in-flight operations after resetQueues bumps generation", async () => {
    let view = makeView("run-a", [makeClipRow("clip-1", 0)]);
    let resolvePatch: ((row: DatasetClipLabClipRow) => void) | null = null;
    const onRowUpdated = vi.fn();

    const coordinator = createClipLabPatchCoordinator({
      getView: () => view,
      patchClip: async () =>
        await new Promise<DatasetClipLabClipRow>((resolve) => {
          resolvePatch = resolve;
        }),
      onViewChange: (next) => {
        view = next;
      },
      onRowUpdated,
      onConflict: async () => {},
    });

    const pending = coordinator.patchDatasetClipLab("run-a", "clip-1", () => ({
      reviewer_tags: ["stale-queued"],
    }));

    await Promise.resolve();
    coordinator.resetQueues();
    resolvePatch?.(makeClipRow("clip-1", 1, ["stale-queued"]));

    await expect(pending).resolves.toMatchObject({ reviewer_tags: ["stale-queued"] });
    expect(view.clips[0]?.reviewer_tags).toEqual([]);
    expect(onRowUpdated).not.toHaveBeenCalled();
  });

  it("rejects queued operations that never start after resetQueues", async () => {
    let view = makeView("run-a", [makeClipRow("clip-1", 0)]);
    let unblockFirst: (() => void) | null = null;
    const firstGate = new Promise<void>((resolve) => {
      unblockFirst = resolve;
    });

    const coordinator = createClipLabPatchCoordinator({
      getView: () => view,
      patchClip: async (_runId, _clipId, payload) => {
        if (payload.reviewer_tags?.includes("first")) {
          await firstGate;
        }
        const current = view.clips.find((clip) => clip.clip_id === "clip-1");
        if (!current) {
          throw new Error("missing clip");
        }
        return makeClipRow("clip-1", current.clip_version + 1, payload.reviewer_tags ?? []);
      },
      onViewChange: (next) => {
        view = next;
      },
      onRowUpdated: () => {},
      onConflict: async () => {},
    });

    const first = coordinator.patchDatasetClipLab("run-a", "clip-1", () => ({
      reviewer_tags: ["first"],
    }));
    const second = coordinator.patchDatasetClipLab("run-a", "clip-1", () => ({
      reviewer_tags: ["second"],
    }));

    await Promise.resolve();
    coordinator.resetQueues();
    unblockFirst?.();
    await first;
    await expect(second).rejects.toThrow(/cancelled/i);
  });

  it("reloads on 409 conflicts through the conflict handler", async () => {
    let view = makeView("run-a", [makeClipRow("clip-1", 0)]);
    const onConflict = vi.fn(async () => {
      view = makeView("run-a", [makeClipRow("clip-1", 2, ["reloaded"])]);
    });

    const coordinator = createClipLabPatchCoordinator({
      getView: () => view,
      patchClip: async () => {
        throw new ApiError("stale clip", 409, "/clip-lab");
      },
      onViewChange: (next) => {
        view = next;
      },
      onRowUpdated: () => {},
      onConflict,
    });

    await expect(
      coordinator.patchDatasetClipLab("run-a", "clip-1", () => ({ review_status: "accepted" })),
    ).rejects.toBeInstanceOf(ApiError);
    expect(onConflict).toHaveBeenCalledWith("run-a", "clip-1");
    expect(view.clips[0]?.reviewer_tags).toEqual(["reloaded"]);
  });
});
