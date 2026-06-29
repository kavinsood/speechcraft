import { describe, expect, it } from "vitest";

import type { DatasetQcPayload, SliceSummary } from "../types";
import {
  buildDatasetQcScoreIndex,
  formatQcScore,
  getReferenceCandidateScore,
  sortClipsForQueue,
} from "./workspace-helpers";

function makeSlice(
  id: string,
  {
    transcriptMatch,
    speakerCheck,
    durationSeconds,
    sourceRecordingId = "recording-a",
    originalStart = 0,
    createdAt = "2026-01-01T00:00:00.000Z",
  }: {
    transcriptMatch: number | null;
    speakerCheck: number | null;
    durationSeconds: number;
    sourceRecordingId?: string;
    originalStart?: number;
    createdAt?: string;
  },
): SliceSummary {
  return {
    id,
    source_recording_id: sourceRecordingId,
    status: "unresolved",
    duration_seconds: durationSeconds,
    model_metadata: {
      transcript_match: transcriptMatch,
      speaker_check: speakerCheck,
      original_start_time: originalStart,
      original_end_time: originalStart + durationSeconds,
    },
    created_at: createdAt,
    tags: [],
    can_undo: false,
    can_redo: false,
  };
}

describe("workspace-helpers reference sorting", () => {
  it("scores a balanced strong clip above a one-sided weaker clip", () => {
    const first = makeSlice("first", { transcriptMatch: 98, speakerCheck: 98, durationSeconds: 6 });
    const second = makeSlice("second", { transcriptMatch: 98, speakerCheck: 80, durationSeconds: 6 });

    const sorted = sortClipsForQueue([second, first], "best_reference_candidate");
    expect(sorted.map((slice) => slice.id)).toEqual(["first", "second"]);
  });

  it("keeps more information than min by preferring 72/99 over 72/72", () => {
    const stronger = makeSlice("stronger", { transcriptMatch: 72, speakerCheck: 99, durationSeconds: 6 });
    const weaker = makeSlice("weaker", { transcriptMatch: 72, speakerCheck: 72, durationSeconds: 6 });

    const sorted = sortClipsForQueue([weaker, stronger], "best_reference_candidate");
    expect(sorted.map((slice) => slice.id)).toEqual(["stronger", "weaker"]);
  });

  it("prefers the sweet-spot duration when scores are otherwise equal", () => {
    const shortClip = makeSlice("short", { transcriptMatch: 90, speakerCheck: 90, durationSeconds: 2 });
    const idealClip = makeSlice("ideal", { transcriptMatch: 90, speakerCheck: 90, durationSeconds: 5 });

    const sorted = sortClipsForQueue([shortClip, idealClip], "best_reference_candidate");
    expect(sorted.map((slice) => slice.id)).toEqual(["ideal", "short"]);
  });

  it("places missing transcript or speaker metrics last in reference mode", () => {
    const scored = makeSlice("scored", { transcriptMatch: 90, speakerCheck: 90, durationSeconds: 6 });
    const missingTranscript = makeSlice("missing-transcript", { transcriptMatch: null, speakerCheck: 90, durationSeconds: 6 });
    const missingSpeaker = makeSlice("missing-speaker", { transcriptMatch: 90, speakerCheck: null, durationSeconds: 6 });

    const sorted = sortClipsForQueue(
      [missingTranscript, scored, missingSpeaker],
      "best_reference_candidate",
    );
    expect(sorted.map((slice) => slice.id)).toEqual(["scored", "missing-transcript", "missing-speaker"]);
  });

  it("uses source order as the deterministic tie-breaker", () => {
    const later = makeSlice("later", { transcriptMatch: 90, speakerCheck: 90, durationSeconds: 6, originalStart: 12 });
    const earlier = makeSlice("earlier", { transcriptMatch: 90, speakerCheck: 90, durationSeconds: 6, originalStart: 3 });

    const sorted = sortClipsForQueue([later, earlier], "best_reference_candidate");
    expect(sorted.map((slice) => slice.id)).toEqual(["earlier", "later"]);
  });

  it("preserves manifest order for source timeline", () => {
    const third = makeSlice("candidate_review_clip_000002", {
      transcriptMatch: 90,
      speakerCheck: 90,
      durationSeconds: 6,
      originalStart: 20,
    });
    const first = makeSlice("candidate_review_clip_000000", {
      transcriptMatch: 90,
      speakerCheck: 90,
      durationSeconds: 6,
      originalStart: 0,
    });
    const second = makeSlice("candidate_review_clip_000001", {
      transcriptMatch: 90,
      speakerCheck: 90,
      durationSeconds: 6,
      originalStart: 10,
    });

    const sorted = sortClipsForQueue([third, first, second], "source_timeline");
    expect(sorted.map((slice) => slice.id)).toEqual([
      "candidate_review_clip_000002",
      "candidate_review_clip_000000",
      "candidate_review_clip_000001",
    ]);
  });

  it("does not mutate the input array", () => {
    const first = makeSlice("first", { transcriptMatch: 85, speakerCheck: 85, durationSeconds: 6 });
    const second = makeSlice("second", { transcriptMatch: 99, speakerCheck: 99, durationSeconds: 6 });
    const clips = [first, second];

    sortClipsForQueue(clips, "best_reference_candidate");
    expect(clips.map((slice) => slice.id)).toEqual(["first", "second"]);
  });

  it("sorts transcript confidence low to high", () => {
    const low = makeSlice("low", { transcriptMatch: 40, speakerCheck: 90, durationSeconds: 6 });
    const high = makeSlice("high", { transcriptMatch: 95, speakerCheck: 90, durationSeconds: 6 });

    const sorted = sortClipsForQueue([high, low], "transcript_confidence");
    expect(sorted.map((slice) => slice.id)).toEqual(["low", "high"]);
  });

  it("sorts speaker purity low to high", () => {
    const low = makeSlice("low", { transcriptMatch: 90, speakerCheck: 45, durationSeconds: 6 });
    const high = makeSlice("high", { transcriptMatch: 90, speakerCheck: 92, durationSeconds: 6 });

    const sorted = sortClipsForQueue([high, low], "speaker_purity");
    expect(sorted.map((slice) => slice.id)).toEqual(["low", "high"]);
  });

  it("uses item_metadata scores when model_metadata exists but does not contain QC fields", () => {
    const hiddenScores = makeSlice("hidden", { transcriptMatch: null, speakerCheck: null, durationSeconds: 6 });
    hiddenScores.model_metadata = {};
    (hiddenScores as unknown as { item_metadata?: Record<string, unknown> }).item_metadata = {
      transcript_match: 92,
      speaker_check: 95,
      original_start_time: 2,
      original_end_time: 8,
    };

    const baseline = makeSlice("baseline", { transcriptMatch: 80, speakerCheck: 80, durationSeconds: 6, originalStart: 4 });
    const sorted = sortClipsForQueue([baseline, hiddenScores], "best_reference_candidate");
    expect(sorted.map((slice) => slice.id)).toEqual(["hidden", "baseline"]);
  });

  it("does not crash on old or malformed runtime slice shapes", () => {
    const malformed = {
      ...makeSlice("malformed", { transcriptMatch: 88, speakerCheck: 87, durationSeconds: 6 }),
      duration_seconds: "6.5" as unknown as number,
      created_at: null as unknown as string,
      source_recording_id: null as unknown as string,
      model_metadata: {
        transcript_match: 88,
        speaker_check: 87,
        original_start_time: "12.2",
        original_end_time: "18.7",
      },
    };

    const clean = makeSlice("clean", {
      transcriptMatch: 90,
      speakerCheck: 90,
      durationSeconds: 6,
      originalStart: 1,
    });

    expect(() => sortClipsForQueue([malformed, clean], "source_timeline")).not.toThrow();
    expect(() => sortClipsForQueue([malformed, clean], "best_reference_candidate")).not.toThrow();
  });

  it("computes the v1 reference score directly from transcript, speaker, and duration", () => {
    expect(getReferenceCandidateScore(100, 60, 6)).toBeCloseTo(75, 6);
    expect(getReferenceCandidateScore(72, 99, 6)).toBeCloseTo(83.368421, 6);
    expect(getReferenceCandidateScore(72, 72, 6)).toBeCloseTo(72, 6);
    expect(getReferenceCandidateScore(null, 72, 6)).toBeNull();
  });

  it("uses a continuous duration support band", () => {
    expect(getReferenceCandidateScore(90, 90, 2.49)).toBeNull();
    expect(getReferenceCandidateScore(90, 90, 2.5)).toBeCloseTo(54, 6);
    expect(getReferenceCandidateScore(90, 90, 4)).toBeCloseTo(90, 6);
    expect(getReferenceCandidateScore(90, 90, 8)).toBeCloseTo(90, 6);
    expect(getReferenceCandidateScore(90, 90, 12)).toBeCloseTo(85.5, 6);
    expect(getReferenceCandidateScore(90, 90, 12.01)).toBeLessThan(85.5);
    expect(getReferenceCandidateScore(90, 90, 15)).toBeCloseTo(54, 6);
    expect(getReferenceCandidateScore(90, 90, 15.01)).toBeNull();
  });
});

describe("buildDatasetQcScoreIndex", () => {
  it("indexes transcript and speaker scores by clip id", () => {
    const payload: DatasetQcPayload = {
      run_id: "dataset-abc123",
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
          clip_id: "candidate_review_clip_000001",
          audio_path: "artifacts/candidate_review_clips/foo.wav",
          audio_url: "/media/dataset-runs/dataset-abc123/candidate-review/candidate_review_clip_000001.wav",
          duration_sec: 4.2,
          training_text: "hello world",
          transcript_match: 91,
          speaker_check: 74,
          transcript_reason_codes: [],
          speaker_reason_codes: [],
          candidate_reason_codes: [],
          qc_reason_codes: [],
          weak_transcript_spans: [],
          weak_speaker_spans: [],
        },
      ],
    };

    const index = buildDatasetQcScoreIndex(payload);

    expect(index.get("candidate_review_clip_000001")).toEqual({
      transcriptMatch: 91,
      speakerCheck: 74,
    });
  });

  it("returns an empty index when QC payload is unavailable", () => {
    expect(buildDatasetQcScoreIndex(null).size).toBe(0);
    expect(buildDatasetQcScoreIndex(undefined).size).toBe(0);
  });
});

describe("formatQcScore", () => {
  it("formats numeric scores and missing values", () => {
    expect(formatQcScore(91)).toBe("91.00");
    expect(formatQcScore(null)).toBe("—");
  });
});
