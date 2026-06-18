import { describe, expect, it } from "vitest"

import type { DatasetQcClip } from "../types"
import {
  acceptedSample,
  bestRejected,
  combinedSummary,
  finalStatus,
  speakerCurve,
  thresholdStatus,
  transcriptCurve,
  worstKept,
} from "./qcLogic"

function clip(
  clipId: string,
  transcriptMatch: number | null,
  speakerCheck: number | null,
  durationSec: number,
  manualOverride: DatasetQcClip["manual_override"] = null,
): DatasetQcClip {
  return {
    clip_id: clipId,
    audio_path: `artifacts/candidate_review_clips/${clipId}.wav`,
    audio_url: `/media/${clipId}.wav`,
    duration_sec: durationSec,
    training_text: clipId,
    alignment_text: clipId,
    transcript_match: transcriptMatch,
    speaker_check: speakerCheck,
    transcript_reason_codes: [],
    speaker_reason_codes: [],
    candidate_reason_codes: [],
    qc_reason_codes: [],
    weak_transcript_spans: [],
    weak_speaker_spans: [],
    manual_override: manualOverride,
  }
}

describe("qcLogic", () => {
  it("applies AND gate and supports overrides including clear override", () => {
    const base = clip("clip-1", 84, 71, 5)
    expect(thresholdStatus(base, 85, 70)).toBe("rejected")
    expect(finalStatus(base, 85, 70)).toBe("rejected")
    expect(finalStatus(base, 85, 70, "force_keep")).toBe("accepted")
    expect(finalStatus({ ...base, manual_override: "force_reject" }, 85, 70)).toBe("rejected")
    expect(finalStatus({ ...base, manual_override: "force_keep" }, 85, 70, null)).toBe("rejected")
  })

  it("orders worst kept by closest, transcript risk, and speaker risk", () => {
    const clips = [
      clip("clip-a", 86, 90, 4),
      clip("clip-b", 95, 70, 4),
      clip("clip-c", 87, 75, 4),
    ]
    expect(worstKept(clips, 85, 70, "closest").map((entry) => entry.clip.clip_id)).toEqual([
      "clip-b",
      "clip-a",
      "clip-c",
    ])
    expect(worstKept(clips, 85, 70, "transcript").map((entry) => entry.clip.clip_id)).toEqual([
      "clip-a",
      "clip-c",
      "clip-b",
    ])
    expect(worstKept(clips, 85, 70, "speaker").map((entry) => entry.clip.clip_id)).toEqual([
      "clip-b",
      "clip-c",
      "clip-a",
    ])
  })

  it("orders best rejected by closest, transcript-only, and speaker-only", () => {
    const clips = [
      clip("clip-a", 84, 80, 4),
      clip("clip-b", 90, 69, 4),
      clip("clip-c", 83, 68, 4),
    ]
    expect(bestRejected(clips, 85, 70, "closest_to_passing").map((entry) => entry.clip.clip_id)).toEqual([
      "clip-a",
      "clip-b",
      "clip-c",
    ])
    expect(bestRejected(clips, 85, 70, "transcript_only").map((entry) => entry.clip.clip_id)).toEqual([
      "clip-a",
    ])
    expect(bestRejected(clips, 85, 70, "speaker_only").map((entry) => entry.clip.clip_id)).toEqual([
      "clip-b",
    ])
  })

  it("applies local overrides to worst kept", () => {
    const clips = [
      clip("fail", 10, 10, 1),
      clip("pass", 90, 90, 1),
    ]
    expect(
      worstKept(clips, 85, 70, "closest", { fail: "force_keep" }).map((entry) => entry.clip.clip_id),
    ).toContain("fail")
  })

  it("applies local overrides to best rejected", () => {
    const clips = [
      clip("pass", 90, 90, 1),
      clip("fail", 10, 10, 1),
    ]
    expect(
      bestRejected(clips, 85, 70, "closest_to_passing", { pass: "force_reject" }).map(
        (entry) => entry.clip.clip_id,
      ),
    ).toContain("pass")
  })

  it("local null clears persisted clip override", () => {
    const overridden = clip("clip-1", 80, 80, 1, "force_keep")
    expect(finalStatus(overridden, 85, 70)).toBe("accepted")
    expect(finalStatus(overridden, 85, 70, null)).toBe("rejected")
  })

  it("builds monotonic single-metric curves", () => {
    const clips = [
      clip("clip-a", 90, 80, 10),
      clip("clip-b", 70, 60, 5),
      clip("clip-c", null, 75, 2),
    ]
    const transcript = transcriptCurve(clips)
    const speaker = speakerCurve(clips)
    expect(transcript[0].acceptedDurationSec).toBe(15)
    expect(transcript[85].acceptedDurationSec).toBe(10)
    expect(speaker[70].acceptedDurationSec).toBe(12)
    for (let index = 1; index < transcript.length; index += 1) {
      expect(transcript[index].acceptedDurationSec).toBeLessThanOrEqual(transcript[index - 1].acceptedDurationSec)
      expect(speaker[index].acceptedDurationSec).toBeLessThanOrEqual(speaker[index - 1].acceptedDurationSec)
    }
  })

  it("returns all accepted clips when accepted sample size is under limit", () => {
    const clips = [
      clip("clip-a", 90, 90, 1),
      clip("clip-b", 91, 91, 1),
      clip("clip-c", 92, 92, 1),
    ]
    expect(acceptedSample(clips, 85, 70, 12).map((entry) => entry.clip.clip_id)).toEqual([
      "clip-a",
      "clip-b",
      "clip-c",
    ])
  })

  it("spreads accepted sample across the kept set and computes combined summary", () => {
    const clips = Array.from({ length: 20 }, (_, index) =>
      clip(`clip-${index}`, 85 + index, 70 + index, 1),
    )
    const sample = acceptedSample(clips, 85, 70, 12)
    expect(sample).toHaveLength(12)
    expect(sample[0].clip.clip_id).toBe("clip-0")
    expect(sample[sample.length - 1].clip.clip_id).toBe("clip-19")

    const summary = combinedSummary(
      [
        clip("keep", 90, 90, 2),
        clip("reject", 84, 90, 3),
        clip("override", 80, 60, 4),
      ],
      85,
      70,
      { override: "force_keep" },
    )
    expect(summary).toEqual({
      acceptedCount: 2,
      rejectedCount: 1,
      acceptedDurationSec: 6,
      rejectedDurationSec: 3,
    })
  })
})
