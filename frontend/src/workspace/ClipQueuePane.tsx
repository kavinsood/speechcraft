import { useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import type { ClipLabItemRef, ReviewWindowSummary, SliceSummary } from "../types";
import WorkspaceStatePanel from "./WorkspaceStatePanel";
import {
  clipMatchesFilters,
  formatSeconds,
  getSliceDuration,
  getSliceTranscriptText,
  queuePriorityOrder,
  sortClipsForQueue,
  statusLabels,
} from "./workspace-helpers";

type WorkspacePhase = "loading" | "error" | "empty" | "ready";

type ClipQueuePaneProps = {
  workspacePhase: WorkspacePhase;
  workspaceError: string | null;
  workspaceEmptyMessage: string | null;
  clips: SliceSummary[];
  reviewWindows: ReviewWindowSummary[];
  activeClipItem: ClipLabItemRef | null;
  onSelectClipItem: (clipItem: ClipLabItemRef) => void;
  onRetryLoad: () => void;
  onVisibleClipIdsChange: (clipIds: string[]) => void;
  onVisibleReviewWindowIdsChange: (clipIds: string[]) => void;
};

export default function ClipQueuePane({
  workspacePhase,
  workspaceError,
  workspaceEmptyMessage,
  clips,
  reviewWindows,
  activeClipItem,
  onSelectClipItem,
  onRetryLoad,
  onVisibleClipIdsChange,
  onVisibleReviewWindowIdsChange,
}: ClipQueuePaneProps) {
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedFilterTags, setSelectedFilterTags] = useState<string[]>([]);
  const [isTagFilterMenuOpen, setIsTagFilterMenuOpen] = useState(false);
  const [hideResolved] = useState(false);
  const deferredSearch = useDeferredValue(searchQuery.trim().toLowerCase());
  const tagFilterRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!isTagFilterMenuOpen) {
      return;
    }

    const handlePointerDown = (event: MouseEvent) => {
      if (!tagFilterRef.current) {
        return;
      }
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }
      if (!tagFilterRef.current.contains(target)) {
        setIsTagFilterMenuOpen(false);
      }
    };

    document.addEventListener("mousedown", handlePointerDown);
    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
    };
  }, [isTagFilterMenuOpen]);

  const availableFilterTags = useMemo(() => {
    const statusTagNames = new Set(queuePriorityOrder.map((status) => status.toLowerCase()));
    const allClipTags = [...clips.flatMap((clip) => clip.tags), ...reviewWindows.flatMap((window) => window.tags)]
      .map((tag) => tag.name.toLowerCase())
      .filter((tagName) => !statusTagNames.has(tagName));
    return Array.from(new Set(allClipTags)).sort();
  }, [clips, reviewWindows]);

  const queueClips = useMemo(() => {
    return sortClipsForQueue(clips).filter((clip) =>
      clipMatchesFilters(clip, deferredSearch, selectedFilterTags, hideResolved),
    );
  }, [clips, deferredSearch, selectedFilterTags, hideResolved]);

  useEffect(() => {
    onVisibleClipIdsChange(queueClips.map((clip) => clip.id));
  }, [queueClips, onVisibleClipIdsChange]);

  const queueReviewWindows = useMemo(() => {
    return [...reviewWindows]
      .sort((left, right) => {
        if (left.source_recording_id !== right.source_recording_id) {
          return left.source_recording_id.localeCompare(right.source_recording_id);
        }
        if (left.order_index !== right.order_index) {
          return left.order_index - right.order_index;
        }
        return left.created_at.localeCompare(right.created_at);
      })
      .filter((window) => {
        if (
          selectedFilterTags.length > 0 &&
          !selectedFilterTags.some((selectedTag) =>
            window.tags.some((tag) => tag.name.toLowerCase() === selectedTag),
          )
        ) {
          return false;
        }
        if (!deferredSearch) {
          return true;
        }
        const haystacks = [
          window.id,
          window.reviewed_transcript || window.rough_transcript,
          window.review_status,
          ...window.tags.map((tag) => tag.name),
        ];
        return haystacks.some((value) => value.toLowerCase().includes(deferredSearch));
      });
  }, [reviewWindows, deferredSearch, selectedFilterTags]);

  useEffect(() => {
    onVisibleReviewWindowIdsChange(queueReviewWindows.map((window) => window.id));
  }, [queueReviewWindows, onVisibleReviewWindowIdsChange]);

  function toggleFilterTag(tagName: string) {
    setSelectedFilterTags((current) =>
      current.includes(tagName)
        ? current.filter((entry) => entry !== tagName)
        : [...current, tagName],
    );
  }

  return (
    <aside className="clip-queue panel">
      <div className="clip-queue-tools">
        <input
          aria-label="Search clips"
          className="search-input"
          placeholder="Search clips"
          value={searchQuery}
          onChange={(event) => setSearchQuery(event.target.value)}
        />
        <div className="tag-filter-bar" ref={tagFilterRef}>
          <button
            type="button"
            className="tag-filter-trigger"
            onClick={() => setIsTagFilterMenuOpen((current) => !current)}
          >
            {selectedFilterTags.length > 0 ? `Tags (${selectedFilterTags.length})` : "Filter Tags"}
          </button>
          <div className="tag-filter-current">
            {selectedFilterTags.length > 0 ? selectedFilterTags.join(", ") : "All tags"}
          </div>
          {isTagFilterMenuOpen ? (
            <div className="tag-filter-popover">
              <ul className="tag-filter-list">
                {availableFilterTags.map((tagName) => (
                  <li key={`filter-${tagName}`}>
                    <button
                      type="button"
                      className={`tag-filter-item ${selectedFilterTags.includes(tagName) ? "selected" : ""}`}
                      onClick={() => toggleFilterTag(tagName)}
                    >
                      <span>{tagName}</span>
                    </button>
                  </li>
                ))}
              </ul>
              <div className="clip-list-meta">
                <span>
                  {selectedFilterTags.length > 0
                    ? `Filtering: ${selectedFilterTags.join(", ")}`
                    : "No tag filter"}
                </span>
                {selectedFilterTags.length > 0 ? (
                  <button type="button" onClick={() => setSelectedFilterTags([])}>
                    Clear
                  </button>
                ) : null}
              </div>
            </div>
          ) : null}
        </div>
      </div>

      <div className="clip-list">
        {workspacePhase === "loading" ? <div className="empty-state">Loading clips...</div> : null}
        {workspacePhase === "error" ? (
          <WorkspaceStatePanel
            title="Queue unavailable"
            message={workspaceError ?? "The clip queue could not be loaded."}
            actionLabel="Retry load"
            onAction={onRetryLoad}
          />
        ) : null}
        {workspacePhase === "empty" ? (
          <div className="empty-state">
            {workspaceEmptyMessage ?? "No projects are available yet."}
          </div>
        ) : null}
        {workspacePhase === "ready" && queueClips.length === 0 ? (
          queueReviewWindows.length === 0 ? <div className="empty-state">No items match the current filters.</div> : null
        ) : null}
        {workspacePhase === "ready" ? (
          <>
            {queueClips.length > 0 ? <p className="eyebrow">Slices</p> : null}
            {queueClips.map((clip, index) => (
              <button
                key={clip.id}
                className={`clip-list-item ${activeClipItem?.kind === "slice" && clip.id === activeClipItem.id ? "active" : ""}`}
                type="button"
                onClick={() => onSelectClipItem({ kind: "slice", id: clip.id })}
              >
                <div className="clip-list-row">
                  <strong>
                    <span className="order-pill">{index + 1}.</span>
                  </strong>
                  <span className={`review-chip status-${clip.status}`}>
                    {statusLabels[clip.status]}
                  </span>
                </div>
                <p>{getSliceTranscriptText(clip)}</p>
                <div className="clip-list-meta">
                  <span>{formatSeconds(getSliceDuration(clip))}</span>
                  <span>{clip.active_variant_generator_model ?? "source"}</span>
                </div>
              </button>
            ))}

            {queueReviewWindows.length > 0 ? <p className="eyebrow">Review Windows</p> : null}
            {queueReviewWindows.map((window, index) => (
              <button
                key={window.id}
                className={`clip-list-item ${activeClipItem?.kind === "review_window" && window.id === activeClipItem.id ? "active" : ""}`}
                type="button"
                onClick={() => onSelectClipItem({ kind: "review_window", id: window.id })}
              >
                <div className="clip-list-row">
                  <strong>
                    <span className="order-pill">{index + 1}.</span>
                  </strong>
                  <span className={`review-chip status-${window.review_status}`}>
                    {statusLabels[window.review_status]}
                  </span>
                </div>
                <p>{window.reviewed_transcript || window.rough_transcript}</p>
                <div className="clip-list-meta">
                  <span>{formatSeconds(Math.max(window.end_seconds - window.start_seconds, 0))}</span>
                  <span>review window</span>
                </div>
              </button>
            ))}
          </>
        ) : null}
      </div>
    </aside>
  );
}
