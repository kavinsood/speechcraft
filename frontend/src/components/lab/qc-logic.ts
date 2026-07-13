// Pure QC / Dataset-Health logic. Ported from the legacy frontend's
// src/qc/qcLogic.ts and adapted to the live GET /api/dataset-runs/{id}/qc
// clip shape. The two scores (transcript_match, speaker_check) are INDEPENDENT
// necessary gates (AND) — never blended. A null score means "unscored" and is
// treated as rejected, matching the backend (_threshold_status in dataset_qc.py).

export type QcClip = {
  clipId: string;
  durationSec: number;
  transcriptMatch: number | null;
  speakerCheck: number | null;
  reasonCodes: string[];
  trainingText: string;
};

export type ManualOverride = "force_keep" | "force_reject";
export type ThresholdStatus = "accepted" | "rejected";

export type ClipWithMargins = {
  clip: QcClip;
  transcriptMargin: number;
  speakerMargin: number;
  riskMargin: number;
  transcriptGap: number;
  speakerGap: number;
  recoveryGap: number;
};

export type CurvePoint = {
  threshold: number;
  acceptedDurationSec: number;
  acceptedClipCount: number;
};

export type CombinedSummary = {
  acceptedCount: number;
  rejectedCount: number;
  acceptedDurationSec: number;
  rejectedDurationSec: number;
};

export type KeptSort = "risk" | "transcript" | "speaker";
export type RejectedSort = "closest" | "transcript_only" | "speaker_only";

const NEG_INF = Number.NEGATIVE_INFINITY;

function scoreOr(score: number | null | undefined): number {
  return typeof score === "number" && Number.isFinite(score) ? score : NEG_INF;
}

function effectiveOverride(
  clip: QcClip,
  overrides: Record<string, ManualOverride | null | undefined>,
): ManualOverride | null {
  return overrides[clip.clipId] ?? null;
}

export function thresholdStatus(
  clip: QcClip,
  transcriptThreshold: number,
  speakerThreshold: number,
): ThresholdStatus {
  if (scoreOr(clip.transcriptMatch) < transcriptThreshold) return "rejected";
  if (scoreOr(clip.speakerCheck) < speakerThreshold) return "rejected";
  return "accepted";
}

export function finalStatus(
  clip: QcClip,
  transcriptThreshold: number,
  speakerThreshold: number,
  overrides: Record<string, ManualOverride | null | undefined> = {},
): ThresholdStatus {
  const override = effectiveOverride(clip, overrides);
  if (override === "force_keep") return "accepted";
  if (override === "force_reject") return "rejected";
  return thresholdStatus(clip, transcriptThreshold, speakerThreshold);
}

export function margins(
  clip: QcClip,
  transcriptThreshold: number,
  speakerThreshold: number,
): ClipWithMargins {
  const t = scoreOr(clip.transcriptMatch);
  const s = scoreOr(clip.speakerCheck);
  const transcriptMargin = t - transcriptThreshold;
  const speakerMargin = s - speakerThreshold;
  const transcriptGap = Math.max(0, transcriptThreshold - t);
  const speakerGap = Math.max(0, speakerThreshold - s);
  return {
    clip,
    transcriptMargin,
    speakerMargin,
    riskMargin: Math.min(transcriptMargin, speakerMargin),
    transcriptGap,
    speakerGap,
    recoveryGap: Math.max(transcriptGap, speakerGap),
  };
}

function byClipId(a: QcClip, b: QcClip): number {
  return a.clipId.localeCompare(b.clipId);
}

function withMarginsByStatus(
  clips: QcClip[],
  transcriptThreshold: number,
  speakerThreshold: number,
  status: ThresholdStatus,
  overrides: Record<string, ManualOverride | null | undefined>,
): ClipWithMargins[] {
  return clips
    .filter((c) => finalStatus(c, transcriptThreshold, speakerThreshold, overrides) === status)
    .map((c) => margins(c, transcriptThreshold, speakerThreshold));
}

/** Accepted clips ordered so the riskiest kept (closest to failing) come first. */
export function riskiestKept(
  clips: QcClip[],
  transcriptThreshold: number,
  speakerThreshold: number,
  sort: KeptSort = "risk",
  overrides: Record<string, ManualOverride | null | undefined> = {},
): ClipWithMargins[] {
  const kept = withMarginsByStatus(clips, transcriptThreshold, speakerThreshold, "accepted", overrides);
  kept.sort((l, r) => {
    if (sort === "transcript") return l.transcriptMargin - r.transcriptMargin || byClipId(l.clip, r.clip);
    if (sort === "speaker") return l.speakerMargin - r.speakerMargin || byClipId(l.clip, r.clip);
    return l.riskMargin - r.riskMargin || byClipId(l.clip, r.clip);
  });
  return kept;
}

/** Rejected clips ordered so the best rejected (closest to passing) come first. */
export function bestRejected(
  clips: QcClip[],
  transcriptThreshold: number,
  speakerThreshold: number,
  sort: RejectedSort = "closest",
  overrides: Record<string, ManualOverride | null | undefined> = {},
): ClipWithMargins[] {
  const rejected = withMarginsByStatus(clips, transcriptThreshold, speakerThreshold, "rejected", overrides).filter(
    (entry) => {
      if (sort === "transcript_only") {
        return (
          scoreOr(entry.clip.transcriptMatch) < transcriptThreshold &&
          scoreOr(entry.clip.speakerCheck) >= speakerThreshold
        );
      }
      if (sort === "speaker_only") {
        return (
          scoreOr(entry.clip.speakerCheck) < speakerThreshold &&
          scoreOr(entry.clip.transcriptMatch) >= transcriptThreshold
        );
      }
      return true;
    },
  );
  rejected.sort((l, r) => {
    if (sort === "transcript_only") {
      return scoreOr(r.clip.transcriptMatch) - scoreOr(l.clip.transcriptMatch) || byClipId(l.clip, r.clip);
    }
    if (sort === "speaker_only") {
      return scoreOr(r.clip.speakerCheck) - scoreOr(l.clip.speakerCheck) || byClipId(l.clip, r.clip);
    }
    return l.recoveryGap - r.recoveryGap || byClipId(l.clip, r.clip);
  });
  return rejected;
}

export function clampScoreToBucket(score: number | null | undefined): number | null {
  if (typeof score !== "number" || !Number.isFinite(score)) return null;
  const bucket = Math.floor(score);
  if (bucket < 0) return 0;
  if (bucket > 100) return 100;
  return bucket;
}

/**
 * Yield curve: for every integer threshold 0..100, the accepted duration and
 * clip count if that single gate were applied alone. This is the "area under
 * the graph" the threshold handle sweeps.
 */
export function thresholdImpactCurve(
  clips: QcClip[],
  getScore: (clip: QcClip) => number | null | undefined,
): CurvePoint[] {
  const durationAt = new Float64Array(101);
  const countAt = new Uint32Array(101);
  for (const clip of clips) {
    const bucket = clampScoreToBucket(getScore(clip));
    if (bucket === null) continue;
    durationAt[bucket] += clip.durationSec;
    countAt[bucket] += 1;
  }
  const curve: CurvePoint[] = new Array(101);
  let acceptedDurationSec = 0;
  let acceptedClipCount = 0;
  for (let threshold = 100; threshold >= 0; threshold -= 1) {
    acceptedDurationSec += durationAt[threshold];
    acceptedClipCount += countAt[threshold];
    curve[threshold] = {
      threshold,
      acceptedDurationSec: Number(acceptedDurationSec.toFixed(6)),
      acceptedClipCount,
    };
  }
  return curve;
}

export type HistogramBin = {
  /** Inclusive lower edge of the bin, 0..100. */
  start: number;
  /** Exclusive upper edge (except the final bin, which is inclusive of 100). */
  end: number;
  /** Bin center, used as the chart X value. */
  center: number;
  count: number;
};

/** Bin scores into `binCount` equal-width buckets across 0..100. Nulls dropped. */
export function histogram(
  clips: QcClip[],
  getScore: (clip: QcClip) => number | null | undefined,
  binCount = 20,
): HistogramBin[] {
  const width = 100 / binCount;
  const bins: HistogramBin[] = Array.from({ length: binCount }, (_, i) => ({
    start: i * width,
    end: (i + 1) * width,
    center: i * width + width / 2,
    count: 0,
  }));
  for (const clip of clips) {
    const score = getScore(clip);
    if (typeof score !== "number" || !Number.isFinite(score)) continue;
    const clamped = Math.max(0, Math.min(100, score));
    let index = Math.floor(clamped / width);
    if (index >= binCount) index = binCount - 1;
    bins[index].count += 1;
  }
  return bins;
}

/** Count of clips whose score is null/unscored for a given field. */
export function unscoredCount(
  clips: QcClip[],
  getScore: (clip: QcClip) => number | null | undefined,
): number {
  let n = 0;
  for (const clip of clips) {
    const score = getScore(clip);
    if (typeof score !== "number" || !Number.isFinite(score)) n += 1;
  }
  return n;
}

export function combinedSummary(
  clips: QcClip[],
  transcriptThreshold: number,
  speakerThreshold: number,
  overrides: Record<string, ManualOverride | null | undefined> = {},
): CombinedSummary {
  const summary: CombinedSummary = {
    acceptedCount: 0,
    rejectedCount: 0,
    acceptedDurationSec: 0,
    rejectedDurationSec: 0,
  };
  for (const clip of clips) {
    if (finalStatus(clip, transcriptThreshold, speakerThreshold, overrides) === "accepted") {
      summary.acceptedCount += 1;
      summary.acceptedDurationSec += clip.durationSec;
    } else {
      summary.rejectedCount += 1;
      summary.rejectedDurationSec += clip.durationSec;
    }
  }
  summary.acceptedDurationSec = Number(summary.acceptedDurationSec.toFixed(6));
  summary.rejectedDurationSec = Number(summary.rejectedDurationSec.toFixed(6));
  return summary;
}
