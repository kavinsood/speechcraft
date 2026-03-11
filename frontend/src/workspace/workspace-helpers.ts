import type { Clip, ProjectDetail, ReviewStatus } from "../types";

export const queuePriorityOrder: ReviewStatus[] = [
  "candidate",
  "needs_attention",
  "in_review",
  "accepted",
  "rejected",
];

export const statusLabels: Record<ReviewStatus, string> = {
  candidate: "Candidate",
  needs_attention: "Needs Attention",
  in_review: "In Review",
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
  return `${seconds.toString().padStart(2, "0")}.${centiseconds
    .toString()
    .padStart(2, "0")}`;
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

export function recalculateProjectDetail(detail: ProjectDetail): ProjectDetail {
  const accepted = detail.clips.filter((clip) => clip.review_status === "accepted");
  const rejected = detail.clips.filter((clip) => clip.review_status === "rejected");
  const needsAttention = detail.clips.filter(
    (clip) => clip.review_status === "needs_attention",
  );

  return {
    ...detail,
    stats: {
      ...detail.stats,
      total_clips: detail.clips.length,
      accepted_clips: accepted.length,
      rejected_clips: rejected.length,
      needs_attention_clips: needsAttention.length,
      total_duration_seconds: Number(
        detail.clips.reduce((sum, clip) => sum + clip.duration_seconds, 0).toFixed(2),
      ),
      accepted_duration_seconds: Number(
        accepted.reduce((sum, clip) => sum + clip.duration_seconds, 0).toFixed(2),
      ),
    },
  };
}

export function sortClipsForQueue(clips: Clip[]): Clip[] {
  return [...clips].sort((left, right) => {
    const leftPriority = queuePriorityOrder.indexOf(left.review_status);
    const rightPriority = queuePriorityOrder.indexOf(right.review_status);

    if (leftPriority !== rightPriority) {
      return leftPriority - rightPriority;
    }

    if (left.order_index !== right.order_index) {
      return left.order_index - right.order_index;
    }

    return left.created_at.localeCompare(right.created_at);
  });
}

export function buildTagColor(name: string): string {
  const palette = [
    "#8a7a3d",
    "#2f6c8f",
    "#c95f44",
    "#3c8452",
    "#8b5fbf",
    "#9a6a2f",
  ];
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
  clip: Clip,
  query: string,
  selectedFilterTags: string[],
  hideResolved: boolean,
): boolean {
  if (hideResolved && (clip.review_status === "accepted" || clip.review_status === "rejected")) {
    return false;
  }

  if (
    selectedFilterTags.length > 0 &&
    !selectedFilterTags.some(
      (selectedTag) => clip.tags.some((tag) => tag.name.toLowerCase() === selectedTag),
    )
  ) {
    return false;
  }

  if (!query) {
    return true;
  }

  const haystacks = [
    clip.id,
    clip.transcript.text_current,
    clip.review_status,
    clip.speaker_name,
    clip.language,
    ...clip.tags.map((tag) => tag.name),
  ];

  return haystacks.some((value) => value.toLowerCase().includes(query));
}
