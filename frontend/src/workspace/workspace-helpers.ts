import type { AudioVariant, ReviewStatus, Slice } from "../types";

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

export function formatSeconds(value: number): string {
  return `${value.toFixed(2)}s`;
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
  slice: Slice,
  key: string,
  fallback: T,
): T {
  const value = slice.model_metadata?.[key];
  return (value as T | undefined) ?? fallback;
}

export function getSliceTranscriptText(slice: Slice): string {
  return slice.transcript?.modified_text ?? slice.transcript?.original_text ?? "";
}

export function getSliceDuration(slice: Slice): number {
  return Number((slice.duration_seconds ?? 0).toFixed(2));
}

export function getSliceOrderIndex(slice: Slice): number {
  return Number(getSliceMetadata(slice, "order_index", 0));
}

export function getSliceOriginalStart(slice: Slice): number {
  return Number(getSliceMetadata(slice, "original_start_time", 0));
}

export function getSliceOriginalEnd(slice: Slice): number {
  return Number(getSliceMetadata(slice, "original_end_time", getSliceDuration(slice)));
}

export function getSliceSpeakerName(slice: Slice): string {
  return String(getSliceMetadata(slice, "speaker_name", "speaker_a"));
}

export function getSliceLanguage(slice: Slice): string {
  return String(getSliceMetadata(slice, "language", "en"));
}

export function isSliceSuperseded(slice: Slice): boolean {
  return Boolean(getSliceMetadata(slice, "is_superseded", false));
}

export function getAlignmentSource(slice: Slice): string {
  const alignmentData = slice.transcript?.alignment_data;
  if (alignmentData && typeof alignmentData === "object" && "source" in alignmentData) {
    return String(alignmentData.source ?? "manual");
  }
  return "manual";
}

export function getAlignmentConfidence(slice: Slice): number | null {
  const alignmentData = slice.transcript?.alignment_data;
  if (alignmentData && typeof alignmentData === "object" && "confidence" in alignmentData) {
    const raw = alignmentData.confidence;
    return typeof raw === "number" ? raw : null;
  }
  return null;
}

export function getRedoTarget(slice: Slice): string | null {
  if (!slice.active_commit_id) {
    return slice.commits.find((commit) => commit.parent_commit_id == null)?.id ?? null;
  }
  return (
    slice.commits.find((commit) => commit.parent_commit_id === slice.active_commit_id)?.id ?? null
  );
}

export function sortVariantsForHistory(variants: AudioVariant[]): AudioVariant[] {
  return [...variants].sort((left, right) => {
    if (left.is_original !== right.is_original) {
      return left.is_original ? -1 : 1;
    }
    return left.id.localeCompare(right.id);
  });
}

export function sortClipsForQueue(clips: Slice[]): Slice[] {
  return [...clips]
    .filter((slice) => !isSliceSuperseded(slice))
    .sort((left, right) => {
      const leftPriority = queuePriorityOrder.indexOf(left.status);
      const rightPriority = queuePriorityOrder.indexOf(right.status);

      if (leftPriority !== rightPriority) {
        return leftPriority - rightPriority;
      }

      if (getSliceOrderIndex(left) !== getSliceOrderIndex(right)) {
        return getSliceOrderIndex(left) - getSliceOrderIndex(right);
      }

      return left.created_at.localeCompare(right.created_at);
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
  slice: Slice,
  query: string,
  selectedFilterTags: string[],
  hideResolved: boolean,
): boolean {
  if (hideResolved && (slice.status === "accepted" || slice.status === "rejected")) {
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
