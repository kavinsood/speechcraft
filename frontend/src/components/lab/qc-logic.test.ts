import { describe, expect, test } from "bun:test";
import {
  bestRejected,
  combinedSummary,
  histogram,
  riskiestKept,
  thresholdImpactCurve,
  thresholdStatus,
  unscoredCount,
  type QcClip,
} from "./qc-logic";

function clip(
  clipId: string,
  transcriptMatch: number | null,
  speakerCheck: number | null,
  durationSec = 2,
): QcClip {
  return { clipId, transcriptMatch, speakerCheck, durationSec, reasonCodes: [], trainingText: clipId };
}

// transcript threshold 85, speaker threshold 70 (defaults)
const T = 85;
const S = 70;

const clips: QcClip[] = [
  clip("a", 95, 90), // clear accept
  clip("b", 86, 71), // barely accepted (riskiest kept)
  clip("c", 84, 95), // rejected on transcript only (best rejected, transcript_only)
  clip("d", 90, 68), // rejected on speaker only
  clip("e", 40, 30), // clear reject
  clip("f", null, 80), // unscored transcript => rejected
];

describe("thresholdStatus (AND-gate, null = rejected)", () => {
  test("both gates required", () => {
    expect(thresholdStatus(clip("x", 95, 90), T, S)).toBe("accepted");
    expect(thresholdStatus(clip("x", 84, 90), T, S)).toBe("rejected");
    expect(thresholdStatus(clip("x", 95, 69), T, S)).toBe("rejected");
  });
  test("null score is rejected", () => {
    expect(thresholdStatus(clip("x", null, 90), T, S)).toBe("rejected");
    expect(thresholdStatus(clip("x", 95, null), T, S)).toBe("rejected");
  });
});

describe("combinedSummary", () => {
  test("counts accepted vs rejected", () => {
    const s = combinedSummary(clips, T, S);
    expect(s.acceptedCount).toBe(2); // a, b
    expect(s.rejectedCount).toBe(4); // c, d, e, f
    expect(s.acceptedDurationSec).toBe(4);
  });
});

describe("riskiestKept", () => {
  test("accepted clips ordered by smallest risk margin first", () => {
    const kept = riskiestKept(clips, T, S, "risk");
    expect(kept.map((k) => k.clip.clipId)).toEqual(["b", "a"]); // b is closest to failing
  });
});

describe("bestRejected", () => {
  test("closest to passing first (smallest recovery gap)", () => {
    const rejected = bestRejected(clips, T, S, "closest");
    // c (transcript 84, gap 1) and d (speaker 68, gap 2) should lead
    expect(rejected[0].clip.clipId).toBe("c");
  });
  test("transcript_only isolates transcript-only failures", () => {
    const rejected = bestRejected(clips, T, S, "transcript_only");
    // c (84) fails transcript with passing speaker; f (null transcript, speaker 80)
    // is also transcript-only. Ordered by descending transcript score, so c then f.
    expect(rejected.map((r) => r.clip.clipId)).toEqual(["c", "f"]);
  });
  test("speaker_only isolates speaker-only failures", () => {
    const rejected = bestRejected(clips, T, S, "speaker_only");
    expect(rejected.map((r) => r.clip.clipId)).toEqual(["d"]);
  });
});

describe("thresholdImpactCurve", () => {
  test("accepted count is monotonic non-increasing as threshold rises", () => {
    const curve = thresholdImpactCurve(clips, (c) => c.transcriptMatch);
    expect(curve[0].acceptedClipCount).toBeGreaterThanOrEqual(curve[100].acceptedClipCount);
    // at threshold 0, every scored clip counts (5 of 6; f is null)
    expect(curve[0].acceptedClipCount).toBe(5);
  });
});

describe("histogram", () => {
  test("bins scored clips, drops nulls", () => {
    const bins = histogram(clips, (c) => c.transcriptMatch, 10);
    const total = bins.reduce((n, b) => n + b.count, 0);
    expect(total).toBe(5); // f (null) excluded
    expect(unscoredCount(clips, (c) => c.transcriptMatch)).toBe(1);
  });
});
