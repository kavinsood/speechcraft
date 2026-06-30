import { describe, expect, it } from "vitest";

import type { ClipLabItem, DatasetClipLabClipRow, DatasetClipLabView } from "../types";
import {
  buildDatasetTagReadOnlyConfig,
  clipLabRowToDisplayTags,
  isDatasetClipLabEditable,
  resolveDatasetClipTags,
} from "./labelPageDatasetHelpers";
import { createClipLabPatchCoordinator } from "../workspace/dataset-clip-lab-patch";

function makeClipRow(overrides: Partial<DatasetClipLabClipRow> = {}): DatasetClipLabClipRow {
  return {
    clip_id: "candidate_review_clip_000001",
    clip_version: 0,
    review_status: "accepted",
    transcript: "Hello world.",
    original_transcript: "Hello world.",
    transcript_override: null,
    reviewer_tags: ["good energy"],
    pipeline_findings: [{ code: "contains_oov", label: "contains OOV" }],
    content_hash: "hash-1",
    accepted_content_hash: "hash-1",
    accepted_at: "2026-01-01T00:00:00Z",
    acceptance_stale: false,
    transcript_match: 0.95,
    speaker_check: 0.9,
    sample_rate_hz: 16000,
    effective_audio_kind: "candidate_original",
    effective_audio_revision_key: "source-sha",
    source_audio_sha256: "source-sha",
    audio_revision_hash: null,
    rendered_audio_sha256: null,
    audio_url: "/media/dataset-runs/run-a/clip-lab/candidate_review_clip_000001/audio/source-sha.wav",
    waveform_peaks_url:
      "/api/dataset-runs/run-a/clips/candidate_review_clip_000001/waveform-peaks/source-sha",
    current_duration_sec: 1.0,
    audio_edit_op_count: 0,
    audio_edit_ops: [],
    can_undo_audio: false,
    can_redo_audio: false,
    render_status: "ready",
    ...overrides,
  };
}

function makeView(overrides: Partial<DatasetClipLabView> = {}): DatasetClipLabView {
  return {
    run_id: "run-a",
    candidate_manifest_sha256: "manifest-sha",
    stale_state: false,
    stale_reason: null,
    invalid_state: false,
    invalid_state_reason: null,
    saved_state_clip_count: 1,
    qc_available: true,
    qc_error: null,
    clips: [makeClipRow()],
    ...overrides,
  };
}

describe("labelPageDatasetHelpers", () => {
  it("treats stale clip lab state as read-only", () => {
    expect(
      isDatasetClipLabEditable(true, "ready", makeView({ stale_state: true, stale_reason: "manifest changed" })),
    ).toBe(false);
  });

  it("treats invalid clip lab state as read-only", () => {
    expect(
      isDatasetClipLabEditable(true, "ready", makeView({ invalid_state: true, invalid_state_reason: "bad json" })),
    ).toBe(false);
  });

  it("treats ready load state without a loaded view as read-only", () => {
    expect(isDatasetClipLabEditable(true, "ready", null)).toBe(false);
  });

  it("builds read-only messaging for stale, invalid, and unavailable states", () => {
    const activeClip = {
      status: "accepted",
      tags: clipLabRowToDisplayTags(makeClipRow()),
    } as ClipLabItem;

    expect(
      buildDatasetTagReadOnlyConfig(true, false, activeClip, makeView({ stale_state: true }), "ready")?.message,
    ).toContain("older candidate manifest");

    expect(
      buildDatasetTagReadOnlyConfig(true, false, activeClip, makeView({ invalid_state: true }), "ready")?.message,
    ).toContain("invalid");

    expect(
      buildDatasetTagReadOnlyConfig(true, false, activeClip, null, "unavailable")?.message,
    ).toContain("unavailable");
  });

  it("merges machine findings and reviewer tags for queue surfaces", () => {
    const tags = resolveDatasetClipTags({ id: "candidate_review_clip_000001" }, makeClipRow());
    expect(tags.map((tag) => tag.name)).toEqual(["contains OOV", "good energy"]);
  });

  it("serializes a new reviewer tag from the latest row when patches are queued", async () => {
    let view = makeView();
    const patchCalls: Array<{ clipVersion: number; reviewerTags?: string[] }> = [];

    const coordinator = createClipLabPatchCoordinator({
      getView: () => view,
      patchClip: async (_runId, _clipId, payload) => {
        patchCalls.push({
          clipVersion: payload.expected_clip_version,
          reviewerTags: payload.reviewer_tags,
        });
        const current = view.clips[0];
        return makeClipRow({
          clip_version: current.clip_version + 1,
          reviewer_tags: payload.reviewer_tags ?? current.reviewer_tags,
        });
      },
      onViewChange: (next) => {
        view = next;
      },
      onRowUpdated: () => {},
      onConflict: async () => {},
    });

    await Promise.all([
      coordinator.patchDatasetClipLab("run-a", "candidate_review_clip_000001", (row) => ({
        reviewer_tags: [...row.reviewer_tags, "mouth noise"],
      })),
      coordinator.patchDatasetClipLab("run-a", "candidate_review_clip_000001", (row) => ({
        reviewer_tags: [...row.reviewer_tags, "breathy"],
      })),
    ]);

    expect(patchCalls).toEqual([
      { clipVersion: 0, reviewerTags: ["good energy", "mouth noise"] },
      { clipVersion: 1, reviewerTags: ["good energy", "mouth noise", "breathy"] },
    ]);
    expect(view.clips[0]?.reviewer_tags).toEqual(["good energy", "mouth noise", "breathy"]);
  });
});
