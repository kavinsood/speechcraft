import { describe, expect, it } from "vitest";

import {
  datasetAudioOperationFromEdlPayload,
  deleteRangeOperation,
  endSecondsToSample,
  insertSilenceOperation,
  pointSecondsToSample,
  requireDatasetSampleRateHz,
  startSecondsToSample,
} from "./audioSampleCoords";

const SAMPLE_RATE = 16000;

describe("audioSampleCoords", () => {
  it("maps delete boundaries with floor and ceil", () => {
    expect(startSecondsToSample(1.00001, SAMPLE_RATE)).toBe(16000);
    expect(endSecondsToSample(1.00001, SAMPLE_RATE)).toBe(16001);
    expect(deleteRangeOperation(1.00001, 1.00001, SAMPLE_RATE)).toEqual({
      kind: "delete_range",
      start_sample: 16000,
      end_sample: 16001,
    });
  });

  it("maps insert coordinates with round", () => {
    expect(pointSecondsToSample(1.00001 * SAMPLE_RATE / SAMPLE_RATE, SAMPLE_RATE)).toBe(16000);
    expect(insertSilenceOperation(1.50004, 0.25004, SAMPLE_RATE)).toEqual({
      kind: "insert_silence",
      at_sample: 24001,
      duration_samples: 4001,
    });
  });

  it("rejects invalid sample rates instead of assuming 16000", () => {
    expect(() => requireDatasetSampleRateHz(null, null)).toThrow(/sample rate/i);
    expect(() => requireDatasetSampleRateHz(0, undefined)).toThrow(/sample rate/i);
    expect(() =>
      datasetAudioOperationFromEdlPayload(
        { op: "delete_range", range: { start_seconds: 0, end_seconds: 1 } },
        Number.NaN,
      ),
    ).toThrow();
  });

  it("accepts manifest sample rate when clip row rate is missing", () => {
    expect(requireDatasetSampleRateHz(null, 24000)).toBe(24000);
  });
});
