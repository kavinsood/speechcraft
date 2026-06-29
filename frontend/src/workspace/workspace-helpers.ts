import type { ClipLabItem, ClipLabVariant, DatasetQcPayload, ReviewStatus, Slice, SliceSummary } from "../types";

type ClipLikeSummary = SliceSummary | ClipLabItem;

export const queuePriorityOrder: ReviewStatus[] = [
  "unresolved",
  "quarantined",
  "accepted",
  "rejected",
];

export const statusLabels: Record<ReviewStatus, string> = {
  unresolved: "Unresolved",
  quarantined: "Quarantined",
  accepted: "Accepted",
  rejected: "Rejected",
};

export type ClipQueueSortMode =
  | "source_timeline"
  | "best_reference_candidate"
  | "transcript_confidence"
  | "speaker_purity"
  | "longest_first";

export type DatasetQcScores = {
  transcriptMatch: number | null;
  speakerCheck: number | null;
};

export function buildDatasetQcScoreIndex(
  payload: DatasetQcPayload | null | undefined,
): Map<string, DatasetQcScores> {
  const index = new Map<string, DatasetQcScores>();
  for (const clip of payload?.clips ?? []) {
    index.set(clip.clip_id, {
      transcriptMatch: clip.transcript_match,
      speakerCheck: clip.speaker_check,
    });
  }
  return index;
}

export function formatQcScore(score: number | null): string {
  return score === null ? "—" : score.toFixed(2);
}

export const clipQueueSortOptions: Array<{ value: ClipQueueSortMode; label: string }> = [
  { value: "source_timeline", label: "Source Order" },
  { value: "best_reference_candidate", label: "Reference Clip Candidates" },
  { value: "transcript_confidence", label: "Transcript Confidence" },
  { value: "speaker_purity", label: "Speaker Purity" },
  { value: "longest_first", label: "Longest First" },
];

export function formatSeconds(value: number): string {
  return `${value.toFixed(2)}s`;
}

function finiteNumberOr(value: unknown, fallback: number): number {
  const numeric = typeof value === "number" ? value : Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

export function formatClipTimestamp(value: number): string {
  const totalCentiseconds = Math.max(0, Math.round(value * 100));
  const seconds = Math.floor(totalCentiseconds / 100);
  const centiseconds = totalCentiseconds % 100;
  return `${seconds.toString().padStart(2, "0")}.${centiseconds.toString().padStart(2, "0")}`;
}

export function formatDurationCompact(totalSeconds: number): string {
  const rounded = Math.max(0, Math.floor(totalSeconds));
  const hours = Math.floor(rounded / 3600);
  const minutes = Math.floor((rounded % 3600) / 60);
  const seconds = rounded % 60;

  if (hours > 0) {
    return `${hours}h ${minutes}m ${seconds}s`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  }
  return `${seconds}s`;
}

export function getSliceMetadata<T>(
  slice: ClipLikeSummary,
  key: string,
  fallback: T,
): T {
  for (const source of metadataSources(slice)) {
    if (key in source) {
      const value = source[key];
      return (value as T | undefined) ?? fallback;
    }
  }
  return fallback;
}

function metadataSources(slice: ClipLikeSummary): Record<string, unknown>[] {
  const sources: Record<string, unknown>[] = [];

  if ("model_metadata" in slice && slice.model_metadata && typeof slice.model_metadata === "object") {
    sources.push(slice.model_metadata);
  }

  if ("item_metadata" in slice && (slice as ClipLabItem).item_metadata && typeof (slice as ClipLabItem).item_metadata === "object") {
    sources.push((slice as ClipLabItem).item_metadata as Record<string, unknown>);
  }

  return sources;
}

function getNestedMetadataValue(source: Record<string, unknown>, path: string[]): unknown {
  let current: unknown = source;
  for (const key of path) {
    if (!current || typeof current !== "object" || !(key in current)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[key];
  }
  return current;
}

function getNumericMetadataValue(slice: ClipLikeSummary, paths: string[][]): number | null {
  for (const source of metadataSources(slice)) {
    for (const path of paths) {
      const raw = getNestedMetadataValue(source, path);
      if (typeof raw === "number" && Number.isFinite(raw)) {
        return raw;
      }
    }
  }
  return null;
}

export function getSliceTranscriptText(slice: ClipLikeSummary): string {
  return slice.transcript?.modified_text ?? slice.transcript?.original_text ?? "";
}

export function getSliceDuration(slice: ClipLikeSummary): number {
  return Number(finiteNumberOr(slice.duration_seconds, 0).toFixed(2));
}

export function getSliceOrderIndex(slice: ClipLikeSummary): number {
  return finiteNumberOr(getSliceMetadata(slice, "order_index", 0), 0);
}

export function getSliceOriginalStart(slice: ClipLikeSummary): number {
  if ("start_seconds" in slice) {
    return finiteNumberOr(slice.start_seconds, 0);
  }

  const startTime = getSliceMetadata<number | null>(slice, "original_start_time", null);
  if (typeof startTime === "number" && Number.isFinite(startTime)) {
    return startTime;
  }

  const startSample = getSliceMetadata<number | null>(slice, "source_start_sample", null);
  const sampleRate = getSliceMetadata<number | null>(slice, "sample_rate", null);
  if (
    typeof startSample === "number"
    && Number.isFinite(startSample)
    && typeof sampleRate === "number"
    && Number.isFinite(sampleRate)
    && sampleRate > 0
  ) {
    return startSample / sampleRate;
  }

  return 0;
}

export function getSliceOriginalEnd(slice: ClipLikeSummary): number {
  if ("end_seconds" in slice) {
    return finiteNumberOr(slice.end_seconds, getSliceDuration(slice));
  }

  const endTime = getSliceMetadata<number | null>(slice, "original_end_time", null);
  if (typeof endTime === "number" && Number.isFinite(endTime)) {
    return endTime;
  }

  const endSample = getSliceMetadata<number | null>(slice, "source_end_sample", null);
  const sampleRate = getSliceMetadata<number | null>(slice, "sample_rate", null);
  if (
    typeof endSample === "number"
    && Number.isFinite(endSample)
    && typeof sampleRate === "number"
    && Number.isFinite(sampleRate)
    && sampleRate > 0
  ) {
    return endSample / sampleRate;
  }

  return finiteNumberOr(
    getSliceMetadata(slice, "original_end_time", getSliceDuration(slice)),
    getSliceDuration(slice),
  );
}

function compareSourceTimeline(left: ClipLikeSummary, right: ClipLikeSummary): number {
  const leftRecording = String(left.source_recording_id ?? "");
  const rightRecording = String(right.source_recording_id ?? "");
  if (leftRecording !== rightRecording) {
    return leftRecording.localeCompare(rightRecording);
  }

  const leftStart = getSliceOriginalStart(left);
  const rightStart = getSliceOriginalStart(right);
  if (leftStart !== rightStart) {
    return leftStart - rightStart;
  }

  const leftEnd = getSliceOriginalEnd(left);
  const rightEnd = getSliceOriginalEnd(right);
  if (leftEnd !== rightEnd) {
    return leftEnd - rightEnd;
  }

  const leftOrder = getSliceOrderIndex(left);
  const rightOrder = getSliceOrderIndex(right);
  if (leftOrder !== rightOrder) {
    return leftOrder - rightOrder;
  }

  return String(left.created_at ?? "").localeCompare(String(right.created_at ?? ""));
}

function compareNumericMetric(
  left: ClipLikeSummary,
  right: ClipLikeSummary,
  metric: (slice: ClipLikeSummary) => number | null,
  direction: "asc" | "desc",
): number {
  const leftValue = metric(left);
  const rightValue = metric(right);
  const leftMissing = leftValue === null;
  const rightMissing = rightValue === null;

  if (leftMissing || rightMissing) {
    if (leftMissing && rightMissing) {
      return compareSourceTimeline(left, right);
    }
    return leftMissing ? 1 : -1;
  }

  if (leftValue !== rightValue) {
    return direction === "asc" ? leftValue - rightValue : rightValue - leftValue;
  }

  return compareSourceTimeline(left, right);
}

export function getSliceSpeakerName(slice: ClipLikeSummary): string {
  if ("speaker_name" in slice && slice.speaker_name) {
    return slice.speaker_name;
  }
  return String(getSliceMetadata(slice, "speaker_name", "speaker_a"));
}

export function getSliceLanguage(slice: ClipLikeSummary): string {
  if ("language" in slice && slice.language) {
    return slice.language;
  }
  return String(getSliceMetadata(slice, "language", "en"));
}

function harmonicMean(left: number, right: number): number {
  if (left <= 0 || right <= 0) {
    return 0;
  }
  return (2 * left * right) / (left + right);
}

function referenceDurationMultiplier(durationSec: number): number | null {
  if (!Number.isFinite(durationSec) || durationSec < 2.5 || durationSec > 15) {
    return null;
  }

  if (durationSec < 4) {
    return 0.6 + 0.4 * ((durationSec - 2.5) / 1.5);
  }

  if (durationSec <= 8) {
    return 1.0;
  }

  if (durationSec <= 12) {
    return 1.0 - 0.05 * ((durationSec - 8) / 4);
  }

  return 0.95 - 0.35 * ((durationSec - 12) / 3);
}

export function getSliceTranscriptConfidence(slice: ClipLikeSummary): number | null {
  return getNumericMetadataValue(slice, [
    ["transcript_match"],
    ["transcript_match_score"],
    ["transcript_score"],
  ]);
}

export function getSliceSpeakerPurityScore(slice: ClipLikeSummary): number | null {
  return getNumericMetadataValue(slice, [
    ["speaker_check"],
    ["speaker_check_score"],
    ["speaker_score"],
  ]);
}

export function getReferenceCandidateScore(
  transcriptMatch: number | null,
  speakerCheck: number | null,
  durationSec: number,
): number | null {
  if (
    transcriptMatch === null
    || speakerCheck === null
    || !Number.isFinite(transcriptMatch)
    || !Number.isFinite(speakerCheck)
  ) {
    return null;
  }

  const durationMultiplier = referenceDurationMultiplier(durationSec);
  if (durationMultiplier === null) {
    return null;
  }

  const transcript = Math.max(0, Math.min(100, transcriptMatch));
  const speaker = Math.max(0, Math.min(100, speakerCheck));
  return harmonicMean(transcript, speaker) * durationMultiplier;
}

export function getSliceReferenceCandidateScore(slice: ClipLikeSummary): number | null {
  return getReferenceCandidateScore(
    getSliceTranscriptConfidence(slice),
    getSliceSpeakerPurityScore(slice),
    getSliceDuration(slice),
  );
}

export function isSliceSuperseded(slice: ClipLikeSummary): boolean {
  return Boolean(getSliceMetadata(slice, "is_superseded", false));
}

export function getAlignmentSource(slice: ClipLikeSummary): string {
  const alignmentData =
    slice.transcript && "alignment_data" in slice.transcript
      ? slice.transcript.alignment_data
      : undefined;
  if (alignmentData && typeof alignmentData === "object" && "source" in alignmentData) {
    return String(alignmentData.source ?? "manual");
  }
  return "manual";
}

export function getAlignmentConfidence(slice: ClipLikeSummary): number | null {
  const alignmentData =
    slice.transcript && "alignment_data" in slice.transcript
      ? slice.transcript.alignment_data
      : undefined;
  if (alignmentData && typeof alignmentData === "object" && "confidence" in alignmentData) {
    const raw = alignmentData.confidence;
    return typeof raw === "number" ? raw : null;
  }
  return null;
}

export function getSliceAudioRevisionKey(slice: Slice): string {
  return JSON.stringify({
    active_variant_id: slice.active_variant_id ?? null,
    edl_operations: slice.active_commit?.edl_operations ?? [],
  });
}

export function getRedoTarget(slice: Slice): string | null {
  if (!slice.active_commit_id) {
    return slice.commits.find((commit) => commit.parent_commit_id == null)?.id ?? null;
  }
  return (
    slice.commits.find((commit) => commit.parent_commit_id === slice.active_commit_id)?.id ?? null
  );
}

export function sortVariantsForHistory(variants: ClipLabVariant[]): ClipLabVariant[] {
  return [...variants].sort((left, right) => {
    if (left.is_original !== right.is_original) {
      return left.is_original ? -1 : 1;
    }
    return left.id.localeCompare(right.id);
  });
}

export function sortClipsForQueue<T extends SliceSummary>(
  clips: T[],
  mode: ClipQueueSortMode = "source_timeline",
): T[] {
  const visible = clips.filter((slice) => !isSliceSuperseded(slice));
  if (mode === "source_timeline") {
    return visible;
  }

  return [...visible].sort((left, right) => {
    if (mode === "best_reference_candidate") {
      return compareNumericMetric(left, right, getSliceReferenceCandidateScore, "desc");
    }

    if (mode === "transcript_confidence") {
      return compareNumericMetric(left, right, getSliceTranscriptConfidence, "asc");
    }

    if (mode === "speaker_purity") {
      return compareNumericMetric(left, right, getSliceSpeakerPurityScore, "asc");
    }

    if (mode === "longest_first") {
      const durationDelta = getSliceDuration(right) - getSliceDuration(left);
      if (durationDelta !== 0) {
        return durationDelta;
      }
      return compareSourceTimeline(left, right);
    }

    return compareSourceTimeline(left, right);
  });
}

export function buildTagColor(name: string): string {
  const palette = ["#8a7a3d", "#2f6c8f", "#c95f44", "#3c8452", "#8b5fbf", "#9a6a2f"];
  const seed = name.split("").reduce((sum, char) => sum + char.charCodeAt(0), 0);
  return palette[seed % palette.length];
}

export function parseTagDraft(value: string): { name: string; color: string }[] {
  const seen = new Set<string>();

  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .filter((entry) => {
      const normalized = entry.toLowerCase();
      if (seen.has(normalized)) {
        return false;
      }
      seen.add(normalized);
      return true;
    })
    .map((name) => ({
      name,
      color: buildTagColor(name),
    }));
}

export function clipMatchesFilters(
  slice: SliceSummary,
  query: string,
  selectedFilterTags: string[],
  selectedFilterStatuses: ReviewStatus[],
  hideResolved: boolean,
): boolean {
  if (hideResolved && (slice.status === "accepted" || slice.status === "rejected")) {
    return false;
  }

  if (selectedFilterStatuses.length > 0 && !selectedFilterStatuses.includes(slice.status)) {
    return false;
  }

  if (
    selectedFilterTags.length > 0 &&
    !selectedFilterTags.some((selectedTag) =>
      slice.tags.some((tag) => tag.name.toLowerCase() === selectedTag),
    )
  ) {
    return false;
  }

  if (!query) {
    return true;
  }

  const haystacks = [
    slice.id,
    getSliceTranscriptText(slice),
    slice.status,
    getSliceSpeakerName(slice),
    getSliceLanguage(slice),
    ...slice.tags.map((tag) => tag.name),
  ];

  return haystacks.some((value) => value.toLowerCase().includes(query));
}
