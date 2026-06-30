import type { ClipLabItem, DatasetClipLabClipRow, DatasetClipLabView, ReviewStatus, Tag } from "../types";

export type DatasetClipLabLoadState = "idle" | "loading" | "ready" | "unavailable";

type DatasetCandidateClip = Record<string, unknown>;

export function isDatasetClipLabEditable(
  datasetMode: boolean,
  loadState: DatasetClipLabLoadState,
  view: DatasetClipLabView | null,
): boolean {
  return (
    datasetMode
    && loadState === "ready"
    && view !== null
    && !view.stale_state
    && !view.invalid_state
  );
}

export function buildDatasetTagReadOnlyMessage(
  view: DatasetClipLabView | null,
  loadState: DatasetClipLabLoadState,
): string {
  if (view?.stale_state) {
    return "Clip Lab state belongs to an older candidate manifest and cannot be edited.";
  }
  if (view?.invalid_state) {
    return "Clip Lab state is invalid and cannot be edited.";
  }
  if (loadState === "unavailable") {
    return "Clip Lab state unavailable. Reload the workspace before editing.";
  }
  return "Clip Lab review edits are unavailable right now.";
}

export function buildDatasetTagReadOnlyConfig(
  datasetMode: boolean,
  editable: boolean,
  activeClip: ClipLabItem | null,
  view: DatasetClipLabView | null,
  loadState: DatasetClipLabLoadState,
): { reviewStatus: ReviewStatus; tags: Tag[]; message: string } | undefined {
  if (!datasetMode || editable || !activeClip) {
    return undefined;
  }

  return {
    reviewStatus: activeClip.status,
    tags: activeClip.tags.filter(isReviewerDisplayTag),
    message: buildDatasetTagReadOnlyMessage(view, loadState),
  };
}

export function manifestFallbackTags(row: DatasetCandidateClip): Tag[] {
  const reasonCodes = Array.isArray(row.review_reason_codes) ? row.review_reason_codes : [];
  return reasonCodes.map((reason, index) => ({
    id: `${String(row.id)}-tag-${index}`,
    name: String(reason).replace(/_/g, " "),
    color: "#7c3aed",
  }));
}

export function clipLabRowToReviewerDisplayTags(row: DatasetClipLabClipRow): Tag[] {
  return row.reviewer_tags.map((name, index) => ({
    id: `${row.clip_id}-reviewer-${index}`,
    name,
    color: "#7c3aed",
  }));
}

export function clipLabRowToDisplayTags(row: DatasetClipLabClipRow): Tag[] {
  // Review status lives on slice.status and inspector controls, not queue tag strips.
  const machine = row.pipeline_findings.map((finding, index) => ({
    id: `${row.clip_id}-finding-${index}`,
    name: finding.label,
    color: "#7c3aed",
  }));
  const human = clipLabRowToReviewerDisplayTags(row);
  return [...machine, ...human];
}

export function isReviewerDisplayTag(tag: Tag): boolean {
  return tag.id.includes("-reviewer-");
}

export function resolveDatasetClipTags(
  row: DatasetCandidateClip,
  clipLabRow?: DatasetClipLabClipRow,
): Tag[] {
  return clipLabRow ? clipLabRowToDisplayTags(clipLabRow) : manifestFallbackTags(row);
}
