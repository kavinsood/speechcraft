import { useMemo, useState } from "react";

import type { DatasetClipLabPipelineFinding, ReviewStatus } from "../types";
import { filterTagSuggestions } from "./dataset-clip-lab-patch";

const RESERVED_REVIEW_STATUS_LABELS = new Set<ReviewStatus>([
  "accepted",
  "rejected",
  "quarantined",
  "unresolved",
]);

type TagComposerProps = {
  reviewStatus: ReviewStatus;
  machineFindings: DatasetClipLabPipelineFinding[];
  reviewerTags: string[];
  suggestions: string[];
  disabled?: boolean;
  acceptanceStale?: boolean;
  onReviewStatusChange: (status: ReviewStatus) => void | Promise<void>;
  onAddReviewerTag: (tag: string) => void | Promise<void>;
  onRemoveReviewerTag: (tag: string) => void | Promise<void>;
};

export function normalizeTagLabel(value: string): string {
  return value.trim().toLowerCase();
}

export function parseStatusLabel(value: string): ReviewStatus | null {
  const normalized = normalizeTagLabel(value);
  return RESERVED_REVIEW_STATUS_LABELS.has(normalized as ReviewStatus)
    ? (normalized as ReviewStatus)
    : null;
}

function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message;
  }
  return fallback;
}

export default function TagComposer({
  machineFindings,
  reviewerTags,
  suggestions,
  disabled = false,
  acceptanceStale = false,
  onReviewStatusChange,
  onAddReviewerTag,
  onRemoveReviewerTag,
}: TagComposerProps) {
  const [inputDraft, setInputDraft] = useState("");
  const [inputFocused, setInputFocused] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);

  const machineLabels = useMemo(
    () => machineFindings.map((finding) => finding.label),
    [machineFindings],
  );
  const machineLabelSet = useMemo(
    () => new Set(machineLabels.map((label) => normalizeTagLabel(label))),
    [machineLabels],
  );
  const reviewerLabelSet = useMemo(
    () => new Set(reviewerTags.map((tag) => normalizeTagLabel(tag))),
    [reviewerTags],
  );
  const visibleSuggestions = useMemo(() => {
    if (!inputFocused) {
      return [];
    }
    return filterTagSuggestions(suggestions, inputDraft, reviewerTags, machineLabels).filter(
      (tagName) => !parseStatusLabel(tagName),
    );
  }, [inputDraft, inputFocused, machineLabels, reviewerTags, suggestions]);

  async function commitInput(value: string) {
    const trimmed = value.trim();
    if (!trimmed || disabled) {
      return;
    }

    const statusLabel = parseStatusLabel(trimmed);
    if (statusLabel) {
      try {
        await onReviewStatusChange(statusLabel);
        setInputDraft("");
        setNotice(null);
      } catch (error) {
        setInputDraft(trimmed);
        setNotice(getErrorMessage(error, "Could not save status. Try again."));
      }
      return;
    }

    const normalized = normalizeTagLabel(trimmed);
    if (machineLabelSet.has(normalized)) {
      setNotice("That label already exists as a pipeline finding.");
      return;
    }
    if (reviewerLabelSet.has(normalized)) {
      setInputDraft("");
      setNotice(null);
      return;
    }

    try {
      await onAddReviewerTag(trimmed);
      setInputDraft("");
      setNotice(null);
    } catch (error) {
      setInputDraft(trimmed);
      setNotice(getErrorMessage(error, "Could not save tag. Try again."));
    }
  }

  async function removeReviewerTag(tag: string) {
    if (disabled) {
      return;
    }
    try {
      await onRemoveReviewerTag(tag);
      setNotice(null);
    } catch (error) {
      setNotice(getErrorMessage(error, "Could not remove tag. Try again."));
    }
  }

  return (
    <div className="tag-composer-content">
      <div className="selection-header">
        <strong>Custom tags</strong>
      </div>
      {acceptanceStale ? (
        <p className="editor-notice">Accepted state is stale because clip content changed since acceptance.</p>
      ) : null}
      {notice ? <p className="editor-notice">{notice}</p> : null}
      <div className="tag-token-list status-group">
        {reviewerTags.map((tag) => (
          <button
            key={`reviewer-${tag}`}
            type="button"
            className="status-button"
            onClick={() => void removeReviewerTag(tag)}
            title="Remove tag"
            disabled={disabled}
          >
            {tag} ×
          </button>
        ))}
        {reviewerTags.length === 0 ? (
          <span className="muted-copy">No custom tags yet.</span>
        ) : null}
      </div>
      <div className="tag-input-row">
        <input
          className="search-input tag-entry-input"
          value={inputDraft}
          onChange={(event) => setInputDraft(event.target.value)}
          onFocus={() => setInputFocused(true)}
          onBlur={() => {
            window.setTimeout(() => setInputFocused(false), 120);
          }}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              void commitInput(inputDraft);
            }
          }}
          placeholder="Add tag (press Enter)"
          disabled={disabled}
        />
      </div>
      {visibleSuggestions.length > 0 ? (
        <div className="tag-suggestion-wrap">
          {visibleSuggestions.map((tagName) => (
            <button
              key={`suggested-tag-${tagName}`}
              type="button"
              className="tag-suggestion-pill"
              onMouseDown={(event) => event.preventDefault()}
              onClick={() => void commitInput(tagName)}
              disabled={disabled}
            >
              {tagName}
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}
