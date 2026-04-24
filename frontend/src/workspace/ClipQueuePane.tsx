import { useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import type { ClipLabItemRef, QcBucket, ReviewStatus, SliceSummary, SourceRecordingQueue } from "../types";
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
  recordings: SourceRecordingQueue[];
  clips: SliceSummary[];
  qcResultMap?: Map<string, {
    bucket: QcBucket;
    visibleBucket: QcBucket;
    score: number;
    reasonCodes: string[];
    reviewSnapshot?: ReviewStatus | null;
  }> | null;
  activeClipItem: ClipLabItemRef | null;
  onSelectClipItem: (clipItem: ClipLabItemRef) => void;
  onRetryLoad: () => void;
  onVisibleClipIdsChange: (clipIds: string[]) => void;
};

export default function ClipQueuePane({
  workspacePhase,
  workspaceError,
  workspaceEmptyMessage,
  recordings,
  clips,
  qcResultMap,
  activeClipItem,
  onSelectClipItem,
  onRetryLoad,
  onVisibleClipIdsChange,
}: ClipQueuePaneProps) {
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedFilterTags, setSelectedFilterTags] = useState<string[]>([]);
  const [selectedFilterStatuses, setSelectedFilterStatuses] = useState<ReviewStatus[]>([]);
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
    const allClipTags = clips
      .flatMap((clip) => clip.tags)
      .map((tag) => tag.name.toLowerCase())
      .filter((tagName) => !statusTagNames.has(tagName));
    return Array.from(new Set(allClipTags)).sort();
  }, [clips]);

  const queueClips = useMemo(() => {
    const sortedClips = qcResultMap ? clips : sortClipsForQueue(clips);
    return sortedClips.filter((clip) =>
      clipMatchesFilters(clip, deferredSearch, selectedFilterTags, selectedFilterStatuses, hideResolved),
    );
  }, [clips, deferredSearch, selectedFilterTags, selectedFilterStatuses, hideResolved, qcResultMap]);

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

  function toggleFilterStatus(status: ReviewStatus) {
    setSelectedFilterStatuses((current) =>
      current.includes(status)
        ? current.filter((entry) => entry !== status)
        : [...current, status],
    );
  }

  function getRecordingStatusLabel(recording: SourceRecordingQueue): string {
    switch (recording.processing_state) {
      case "transcribing":
        return "Transcribing";
      case "aligning":
        return "Aligning";
      case "slicing":
        return "Slicing";
      case "alignment_stale":
        return "Needs Realignment";
      case "failed":
        return "Failed";
      case "sliced":
        return "Sliced";
      case "aligned":
        return "Aligned";
      case "transcribed":
        return "Transcribed";
      default:
        return "Idle";
    }
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
            {selectedFilterTags.length + selectedFilterStatuses.length > 0
              ? `Filters (${selectedFilterTags.length + selectedFilterStatuses.length})`
              : "Filter Clips"}
          </button>
          <div className="tag-filter-current">
            {selectedFilterStatuses.length > 0 || selectedFilterTags.length > 0
              ? [...selectedFilterStatuses.map((status) => statusLabels[status]), ...selectedFilterTags].join(", ")
              : "All slices"}
          </div>
          {isTagFilterMenuOpen ? (
            <div className="tag-filter-popover">
              <ul className="tag-filter-list">
                {queuePriorityOrder.map((status) => (
                  <li key={`filter-status-${status}`}>
                    <button
                      type="button"
                      className={`tag-filter-item ${selectedFilterStatuses.includes(status) ? "selected" : ""}`}
                      onClick={() => toggleFilterStatus(status)}
                    >
                      <span>{statusLabels[status]}</span>
                    </button>
                  </li>
                ))}
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
              {selectedFilterStatuses.length > 0 || selectedFilterTags.length > 0 ? (
                <div className="clip-list-meta">
                  <button
                    type="button"
                    onClick={() => {
                      setSelectedFilterStatuses([]);
                      setSelectedFilterTags([]);
                    }}
                  >
                    Clear
                  </button>
                </div>
              ) : null}
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
          <div className="workspace-empty-stack">
            <div className="empty-state">
              {workspaceEmptyMessage ?? "No projects are available yet."}
            </div>
            {recordings.length > 0 ? (
              <div className="recording-status-list">
                <p className="eyebrow">Recordings</p>
                {recordings.map((recording) => (
                  <div key={recording.id} className="recording-status-card">
                    <div className="clip-list-row">
                      <strong>{recording.id}</strong>
                      <span className={`review-chip status-${recording.processing_state === "failed" ? "rejected" : recording.processing_state === "sliced" ? "accepted" : "unresolved"}`}>
                        {getRecordingStatusLabel(recording)}
                      </span>
                    </div>
                    <p>{recording.processing_message ?? "Recording is idle."}</p>
                    <div className="clip-list-meta">
                      <span>{formatSeconds(recording.duration_seconds)}</span>
                      <span>{recording.slice_count} slice{recording.slice_count === 1 ? "" : "s"}</span>
                    </div>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        ) : null}
        {workspacePhase === "ready" && queueClips.length === 0 ? (
          <div className="empty-state">No items match the current filters.</div>
        ) : null}
        {workspacePhase === "ready" ? (
          <>
            {queueClips.map((clip, index) => {
              const qcMeta = qcResultMap?.get(clip.id) ?? null;
              return (
                <button
                  key={clip.id}
                  className={`clip-list-item ${clip.id === activeClipItem?.id ? "active" : ""}`}
                  type="button"
                  onClick={() => onSelectClipItem({ id: clip.id })}
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
                  {qcMeta ? (
                    <div className="qc-queue-meta">
                      <span className={`qc-bucket-label qc-bucket-${qcMeta.visibleBucket}`}>
                        QC {qcMeta.visibleBucket.replace(/_/g, " ")}
                      </span>
                      <span>{qcMeta.score.toFixed(3)}</span>
                    </div>
                  ) : null}
                  <div className="clip-list-meta">
                    <span>{formatSeconds(getSliceDuration(clip))}</span>
                    <span>{clip.active_variant_generator_model ?? "source"}</span>
                  </div>
                </button>
              );
            })}
          </>
        ) : null}
      </div>
    </aside>
  );
}
