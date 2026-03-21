import { describe, expect, it } from "vitest";

import { clampTrimOffsets, validateManualTrimOffsets } from "./reference-trim-helpers";

describe("reference-trim-helpers", () => {
  it("clamps non-finite inputs to a safe full-span fallback", () => {
    const suggestion = clampTrimOffsets(Number.NaN, Number.NaN, Number.NaN);

    expect(suggestion.startOffsetSeconds).toBe(0);
    expect(suggestion.endOffsetSeconds).toBeGreaterThan(0);
    expect(Number.isFinite(suggestion.previewDurationSeconds)).toBe(true);
  });

  it("rejects invalid manual trim input", () => {
    const result = validateManualTrimOffsets(Number.NaN, 1.5, 4.5);

    expect(result.trim).toBeNull();
    expect(result.error).toBe("Enter valid numeric trim bounds.");
  });

  it("rejects manual trim outside the candidate duration", () => {
    const result = validateManualTrimOffsets(0.2, 5.2, 4.5);

    expect(result.trim).toBeNull();
    expect(result.error).toBe("Trim end must stay inside the candidate.");
  });

  it("accepts a valid manual trim without silently clamping it", () => {
    const result = validateManualTrimOffsets(0.35, 3.85, 4.5);

    expect(result.error).toBeNull();
    expect(result.trim).toEqual({
      startOffsetSeconds: 0.35,
      endOffsetSeconds: 3.85,
      previewDurationSeconds: 4.5,
      heuristic: "manual",
    });
  });
});
