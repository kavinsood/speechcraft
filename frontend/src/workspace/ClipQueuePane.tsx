import { useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import type { Clip } from "../types";
import WorkspaceStatePanel from "./WorkspaceStatePanel";
import {
  clipMatchesFilters,
  formatSeconds,
  queuePriorityOrder,
  sortClipsForQueue,
  statusLabels,
} from "./workspace-helpers";

type WorkspacePhase = "loading" | "error" | "empty" | "ready";

type ClipQueuePaneProps = {
  workspacePhase: WorkspacePhase;
  workspaceError: string | null;
  workspaceEmptyMessage: string | null;
  clips: Clip[];
  activeClipId: string | null;
  onSelectClip: (clipId: string) => void;
  onRetryLoad: () => void;
  onVisibleClipIdsChange: (clipIds: string[]) => void;
};

export default function ClipQueuePane({
  workspacePhase,
  workspaceError,
  workspaceEmptyMessage,
  clips,
  activeClipId,
  onSelectClip,
  onRetryLoad,
  onVisibleClipIdsChange,
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
    const allClipTags = clips.flatMap((clip) =>
      clip.tags
        .map((tag) => tag.name.toLowerCase())
        .filter((tagName) => !statusTagNames.has(tagName)),
    );
    return Array.from(new Set(allClipTags)).sort();
  }, [clips]);

  const queueClips = useMemo(() => {
    return sortClipsForQueue(clips).filter((clip) =>
      clipMatchesFilters(clip, deferredSearch, selectedFilterTags, hideResolved),
    );
  }, [clips, deferredSearch, selectedFilterTags, hideResolved]);

  useEffect(() => {
    onVisibleClipIdsChange(queueClips.map((clip) => clip.id));
  }, [queueClips, onVisibleClipIdsChange]);

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
          <div className="empty-state">No clips match the current filters.</div>
        ) : null}
        {workspacePhase === "ready"
          ? queueClips.map((clip, index) => (
              <button
                key={clip.id}
                className={`clip-list-item ${clip.id === activeClipId ? "active" : ""}`}
                type="button"
                onClick={() => onSelectClip(clip.id)}
              >
                <div className="clip-list-row">
                  <strong>
                    <span className="order-pill">{index + 1}.</span>
                  </strong>
                  <span className={`review-chip status-${clip.review_status}`}>
                    {statusLabels[clip.review_status]}
                  </span>
                </div>
                <p>{clip.transcript.text_current}</p>
                <div className="clip-list-meta">
                  <span>{formatSeconds(clip.duration_seconds)}</span>
                  <span>{clip.edit_state}</span>
                </div>
              </button>
            ))
          : null}
      </div>
    </aside>
  );
}
