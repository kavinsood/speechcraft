export type DatasetAudioOperation =
  | {
      kind: "delete_range";
      start_sample: number;
      end_sample: number;
    }
  | {
      kind: "insert_silence";
      at_sample: number;
      duration_samples: number;
    };

export function startSecondsToSample(seconds: number, sampleRateHz: number): number {
  if (!Number.isFinite(seconds) || !Number.isFinite(sampleRateHz) || sampleRateHz <= 0) {
    return 0;
  }
  return Math.max(0, Math.floor(seconds * sampleRateHz));
}

export function endSecondsToSample(seconds: number, sampleRateHz: number): number {
  if (!Number.isFinite(seconds) || !Number.isFinite(sampleRateHz) || sampleRateHz <= 0) {
    return 0;
  }
  return Math.max(0, Math.ceil(seconds * sampleRateHz));
}

export function pointSecondsToSample(seconds: number, sampleRateHz: number): number {
  if (!Number.isFinite(seconds) || !Number.isFinite(sampleRateHz) || sampleRateHz <= 0) {
    return 0;
  }
  return Math.max(0, Math.round(seconds * sampleRateHz));
}

export function requireDatasetSampleRateHz(
  sampleRateHz: number | null | undefined,
  manifestSampleRate?: unknown,
): number {
  if (typeof sampleRateHz === "number" && Number.isFinite(sampleRateHz) && sampleRateHz > 0) {
    return sampleRateHz;
  }
  if (
    typeof manifestSampleRate === "number"
    && Number.isFinite(manifestSampleRate)
    && manifestSampleRate > 0
  ) {
    return manifestSampleRate;
  }
  throw new Error("Clip sample rate is unavailable. Reload the workspace before editing audio.");
}

export function deleteRangeOperation(
  startSeconds: number,
  endSeconds: number,
  sampleRateHz: number,
): DatasetAudioOperation {
  return {
    kind: "delete_range",
    start_sample: startSecondsToSample(startSeconds, sampleRateHz),
    end_sample: endSecondsToSample(endSeconds, sampleRateHz),
  };
}

export function insertSilenceOperation(
  atSeconds: number,
  durationSeconds: number,
  sampleRateHz: number,
): DatasetAudioOperation {
  return {
    kind: "insert_silence",
    at_sample: pointSecondsToSample(atSeconds, sampleRateHz),
    duration_samples: pointSecondsToSample(durationSeconds, sampleRateHz),
  };
}

export function datasetAudioOperationFromEdlPayload(
  payload: {
    op: string;
    range?: { start_seconds: number; end_seconds: number } | null;
    duration_seconds?: number | null;
  },
  sampleRateHz: number,
): DatasetAudioOperation {
  if (!Number.isFinite(sampleRateHz) || sampleRateHz <= 0) {
    throw new Error("Clip sample rate is unavailable. Reload the workspace before editing audio.");
  }
  if (payload.op === "delete_range") {
    const start = payload.range?.start_seconds ?? 0;
    const end = payload.range?.end_seconds ?? start;
    return deleteRangeOperation(start, end, sampleRateHz);
  }
  if (payload.op === "insert_silence") {
    const at = payload.range?.start_seconds ?? 0;
    const duration = payload.duration_seconds ?? 0;
    return insertSilenceOperation(at, duration, sampleRateHz);
  }
  throw new Error(`Unsupported dataset audio operation: ${payload.op}`);
}
