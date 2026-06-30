import type { DatasetQcClip, ManualOverride } from "../types"

export type ThresholdStatus = "accepted" | "rejected"
export type AuditListMode =
  | "closest"
  | "transcript"
  | "speaker"
  | "closest_to_passing"
  | "transcript_only"
  | "speaker_only"

export type ClipWithMargins = {
  clip: DatasetQcClip;
  transcriptMargin: number;
  speakerMargin: number;
  riskMargin: number;
  transcriptGap: number;
  speakerGap: number;
  recoveryGap: number;
}

export type CurvePoint = {
  threshold: number;
  acceptedDurationSec: number;
  acceptedClipCount: number;
}

export type CombinedSummary = {
  acceptedCount: number;
  rejectedCount: number;
  acceptedDurationSec: number;
  rejectedDurationSec: number;
}

function scoreOrNegativeInfinity(score: number | null | undefined): number {
  return typeof score === "number" && Number.isFinite(score) ? score : Number.NEGATIVE_INFINITY
}

function effectiveOverride(
  clip: DatasetQcClip,
  override?: ManualOverride | null,
): ManualOverride | null {
  if (override !== undefined) {
    return override
  }
  return clip.manual_override ?? null
}

export function thresholdStatus(
  clip: DatasetQcClip,
  transcriptThreshold: number,
  speakerThreshold: number,
): ThresholdStatus {
  if (scoreOrNegativeInfinity(clip.transcript_match) < transcriptThreshold) {
    return "rejected"
  }
  if (scoreOrNegativeInfinity(clip.speaker_check) < speakerThreshold) {
    return "rejected"
  }
  return "accepted"
}

export function finalStatus(
  clip: DatasetQcClip,
  transcriptThreshold: number,
  speakerThreshold: number,
  override?: ManualOverride | null,
): ThresholdStatus {
  const chosenOverride = effectiveOverride(clip, override)
  if (chosenOverride === "force_keep") return "accepted"
  if (chosenOverride === "force_reject") return "rejected"
  return thresholdStatus(clip, transcriptThreshold, speakerThreshold)
}

export function margins(
  clip: DatasetQcClip,
  transcriptThreshold: number,
  speakerThreshold: number,
): ClipWithMargins {
  const transcriptScore = scoreOrNegativeInfinity(clip.transcript_match)
  const speakerScore = scoreOrNegativeInfinity(clip.speaker_check)
  const transcriptMargin = transcriptScore - transcriptThreshold
  const speakerMargin = speakerScore - speakerThreshold
  const transcriptGap = Math.max(0, transcriptThreshold - transcriptScore)
  const speakerGap = Math.max(0, speakerThreshold - speakerScore)
  return {
    clip,
    transcriptMargin,
    speakerMargin,
    riskMargin: Math.min(transcriptMargin, speakerMargin),
    transcriptGap,
    speakerGap,
    recoveryGap: Math.max(transcriptGap, speakerGap),
  }
}

function compareClipIds(left: DatasetQcClip, right: DatasetQcClip): number {
  return left.clip_id.localeCompare(right.clip_id)
}

export function worstKept(
  clips: DatasetQcClip[],
  transcriptThreshold: number,
  speakerThreshold: number,
  mode: "closest" | "transcript" | "speaker" = "closest",
  overridesByClipId: Record<string, ManualOverride | null> = {},
): ClipWithMargins[] {
  const kept = clipsWithMarginsByFinalStatus(
    clips,
    transcriptThreshold,
    speakerThreshold,
    "accepted",
    overridesByClipId,
  )

  kept.sort((left, right) => {
    if (mode === "transcript") {
      return left.transcriptMargin - right.transcriptMargin || compareClipIds(left.clip, right.clip)
    }
    if (mode === "speaker") {
      return left.speakerMargin - right.speakerMargin || compareClipIds(left.clip, right.clip)
    }
    return left.riskMargin - right.riskMargin || compareClipIds(left.clip, right.clip)
  })
  return kept
}

export function bestRejected(
  clips: DatasetQcClip[],
  transcriptThreshold: number,
  speakerThreshold: number,
  mode: "closest_to_passing" | "transcript_only" | "speaker_only" = "closest_to_passing",
  overridesByClipId: Record<string, ManualOverride | null> = {},
): ClipWithMargins[] {
  const rejected = clipsWithMarginsByFinalStatus(
    clips,
    transcriptThreshold,
    speakerThreshold,
    "rejected",
    overridesByClipId,
  )
    .filter((entry) => {
      if (mode === "transcript_only") {
        return scoreOrNegativeInfinity(entry.clip.transcript_match) < transcriptThreshold
          && scoreOrNegativeInfinity(entry.clip.speaker_check) >= speakerThreshold
      }
      if (mode === "speaker_only") {
        return scoreOrNegativeInfinity(entry.clip.speaker_check) < speakerThreshold
          && scoreOrNegativeInfinity(entry.clip.transcript_match) >= transcriptThreshold
      }
      return true
    })

  rejected.sort((left, right) => {
    if (mode === "transcript_only") {
      return scoreOrNegativeInfinity(right.clip.transcript_match) - scoreOrNegativeInfinity(left.clip.transcript_match)
        || compareClipIds(left.clip, right.clip)
    }
    if (mode === "speaker_only") {
      return scoreOrNegativeInfinity(right.clip.speaker_check) - scoreOrNegativeInfinity(left.clip.speaker_check)
        || compareClipIds(left.clip, right.clip)
    }
    return left.recoveryGap - right.recoveryGap || compareClipIds(left.clip, right.clip)
  })
  return rejected
}

export function acceptedSample(
  clips: DatasetQcClip[],
  transcriptThreshold: number,
  speakerThreshold: number,
  n = 12,
  overridesByClipId: Record<string, ManualOverride | null> = {},
): ClipWithMargins[] {
  const kept = worstKept(
    clips,
    transcriptThreshold,
    speakerThreshold,
    "closest",
    overridesByClipId,
  )
  if (kept.length <= n) {
    return kept
  }
  const result: ClipWithMargins[] = []
  for (let index = 0; index < n; index += 1) {
    const sampleIndex = Math.round((index * (kept.length - 1)) / (n - 1))
    result.push(kept[sampleIndex])
  }
  return result
}

export function clampScoreToBucket(score: number | null | undefined): number | null {
  if (typeof score !== "number" || !Number.isFinite(score)) {
    return null
  }
  const bucket = Math.floor(score)
  if (bucket < 0) return 0
  if (bucket > 100) return 100
  return bucket
}

export function thresholdImpactCurve<T>(
  clips: T[],
  getScore: (clip: T) => number | null | undefined,
  getDurationSec: (clip: T) => number,
): CurvePoint[] {
  const durationAtScore = new Float64Array(101)
  const countAtScore = new Uint32Array(101)
  for (const clip of clips) {
    const bucket = clampScoreToBucket(getScore(clip))
    if (bucket === null) continue
    durationAtScore[bucket] += getDurationSec(clip)
    countAtScore[bucket] += 1
  }
  const curve: CurvePoint[] = new Array(101)
  let acceptedDurationSec = 0
  let acceptedClipCount = 0
  for (let threshold = 100; threshold >= 0; threshold -= 1) {
    acceptedDurationSec += durationAtScore[threshold]
    acceptedClipCount += countAtScore[threshold]
    curve[threshold] = {
      threshold,
      acceptedDurationSec: Number(acceptedDurationSec.toFixed(6)),
      acceptedClipCount,
    }
  }
  return curve
}

export function transcriptCurve(clips: DatasetQcClip[]): CurvePoint[] {
  return thresholdImpactCurve(
    clips,
    (clip) => clip.transcript_match,
    (clip) => clip.duration_sec,
  )
}

export function speakerCurve(clips: DatasetQcClip[]): CurvePoint[] {
  return thresholdImpactCurve(
    clips,
    (clip) => clip.speaker_check,
    (clip) => clip.duration_sec,
  )
}

export function combinedSummary(
  clips: DatasetQcClip[],
  transcriptThreshold: number,
  speakerThreshold: number,
  overridesByClipId: Record<string, ManualOverride | null> = {},
): CombinedSummary {
  const summary = clips.reduce<CombinedSummary>(
    (summary, clip) => {
      const status = finalStatus(
        clip,
        transcriptThreshold,
        speakerThreshold,
        overridesByClipId[clip.clip_id],
      )
      if (status === "accepted") {
        summary.acceptedCount += 1
        summary.acceptedDurationSec += clip.duration_sec
      } else {
        summary.rejectedCount += 1
        summary.rejectedDurationSec += clip.duration_sec
      }
      return summary
    },
    {
      acceptedCount: 0,
      rejectedCount: 0,
      acceptedDurationSec: 0,
      rejectedDurationSec: 0,
    },
  )
  return {
    ...summary,
    acceptedDurationSec: Number(summary.acceptedDurationSec.toFixed(6)),
    rejectedDurationSec: Number(summary.rejectedDurationSec.toFixed(6)),
  }
}

function clipsWithMarginsByFinalStatus(
  clips: DatasetQcClip[],
  transcriptThreshold: number,
  speakerThreshold: number,
  status: ThresholdStatus,
  overridesByClipId: Record<string, ManualOverride | null> = {},
): ClipWithMargins[] {
  return clips
    .filter(
      (clip) =>
        finalStatus(
          clip,
          transcriptThreshold,
          speakerThreshold,
          overridesByClipId[clip.clip_id],
        ) === status,
    )
    .map((clip) => margins(clip, transcriptThreshold, speakerThreshold))
}
