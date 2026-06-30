import { describe, expect, it } from "vitest";

import {
  derivePeaksForActiveRevision,
  derivePeaksLoadStateForRevision,
  EMPTY_REVISION_PEAKS,
  initialRevisionPeaksForFetch,
} from "./revisionPeaksState";

describe("revisionPeaksState", () => {
  it("treats revision mismatch as loading before peaks resolve", () => {
    const revisionPeaks = {
      revisionKey: "rev-a",
      status: "ready" as const,
      peaks: [0.1, 0.2],
    };

    expect(
      derivePeaksLoadStateForRevision(revisionPeaks, "rev-b", {
        waveformPeaksUrl: "/api/peaks/rev-b",
        datasetRenderStatus: "ready",
      }),
    ).toBe("loading");
    expect(derivePeaksForActiveRevision(revisionPeaks, "rev-b")).toBeNull();
  });

  it("starts dataset revisions in loading state with no peaks", () => {
    expect(initialRevisionPeaksForFetch("rev-a")).toEqual({
      revisionKey: "rev-a",
      status: "loading",
      peaks: null,
    });
    expect(
      derivePeaksLoadStateForRevision(EMPTY_REVISION_PEAKS, "rev-a", {
        waveformPeaksUrl: "/api/peaks/rev-a",
        datasetRenderStatus: "ready",
      }),
    ).toBe("loading");
    expect(derivePeaksForActiveRevision(EMPTY_REVISION_PEAKS, "rev-a")).toBeNull();
  });

  it("exposes ready peaks only for the matching revision", () => {
    const revisionPeaks = {
      revisionKey: "rev-a",
      status: "ready" as const,
      peaks: [0.3, 0.4],
    };

    expect(
      derivePeaksLoadStateForRevision(revisionPeaks, "rev-a", {
        waveformPeaksUrl: "/api/peaks/rev-a",
        datasetRenderStatus: "ready",
      }),
    ).toBe("ready");
    expect(derivePeaksForActiveRevision(revisionPeaks, "rev-a")).toEqual([0.3, 0.4]);
  });
});
