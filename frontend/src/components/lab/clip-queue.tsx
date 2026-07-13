"use client";

import { Badge } from "@midday/ui/badge";
import { Button } from "@midday/ui/button";
import { cn } from "@midday/ui/cn";
import { Icons } from "@midday/ui/icons";
import { Input } from "@midday/ui/input";
import { Popover, PopoverContent, PopoverTrigger } from "@midday/ui/popover";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@midday/ui/select";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useRef } from "react";
import {
  type LabClip,
  type MachineBucket,
  MACHINE_BUCKET_LABELS,
  MACHINE_BUCKET_ORDER,
  type ReviewStatus,
  REVIEW_STATUS_ORDER,
  type SortMode,
  SORT_OPTIONS,
  STATUS_LABELS,
  formatSeconds,
} from "./lab-data";
import { StatusBadge } from "./status-badge";

type ClipQueueProps = {
  clips: LabClip[];
  activeClipId: string | null;
  search: string;
  onSearchChange: (value: string) => void;
  statuses: ReviewStatus[];
  tags: string[];
  availableTags: string[];
  buckets: MachineBucket[];
  onToggleStatus: (status: ReviewStatus) => void;
  onToggleTag: (tag: string) => void;
  onToggleBucket: (bucket: MachineBucket) => void;
  onClearFilters: () => void;
  sortMode: SortMode;
  onSortModeChange: (mode: SortMode) => void;
  onSelect: (id: string) => void;
};

export function ClipQueue({
  clips,
  activeClipId,
  search,
  onSearchChange,
  statuses,
  tags,
  availableTags,
  buckets,
  onToggleStatus,
  onToggleTag,
  onToggleBucket,
  onClearFilters,
  sortMode,
  onSortModeChange,
  onSelect,
}: ClipQueueProps) {
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const activeFilterCount = statuses.length + tags.length + buckets.length;

  const virtualizer = useVirtualizer({
    count: clips.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 104,
    overscan: 8,
  });

  const filterSummary =
    activeFilterCount > 0
      ? [
          ...statuses.map((s) => STATUS_LABELS[s]),
          ...buckets.map((b) => MACHINE_BUCKET_LABELS[b]),
          ...tags,
        ].join(", ")
      : "All slices";

  return (
    <aside className="flex h-full w-[320px] flex-shrink-0 flex-col border-r border-border">
      {/* Tools: search + filter */}
      <div className="flex items-center gap-2 p-3">
        <div className="relative flex-1">
          <Icons.Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            aria-label="Search clips"
            placeholder="Search clips"
            value={search}
            onChange={(event) => onSearchChange(event.target.value)}
            className="h-9 pl-8"
          />
        </div>

        <Popover>
          <PopoverTrigger asChild>
            <Button variant="outline" size="sm" className="h-9 gap-1.5">
              <Icons.Filter className="size-4" />
              {activeFilterCount > 0 ? `Filters (${activeFilterCount})` : "Filter"}
            </Button>
          </PopoverTrigger>
          <PopoverContent align="end" className="w-64 p-0">
            <div className="p-3">
              <p className="mb-2 text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Status
              </p>
              <div className="flex flex-wrap gap-1.5">
                {REVIEW_STATUS_ORDER.map((status) => (
                  <button
                    key={status}
                    type="button"
                    onClick={() => onToggleStatus(status)}
                    className={cn(
                      "border px-2 py-1 text-xs transition-colors",
                      statuses.includes(status)
                        ? "border-primary text-primary"
                        : "border-border text-muted-foreground hover:text-foreground",
                    )}
                  >
                    {STATUS_LABELS[status]}
                  </button>
                ))}
              </div>

              <p className="mb-2 mt-4 text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Machine QC
              </p>
              <div className="flex flex-wrap gap-1.5">
                {MACHINE_BUCKET_ORDER.map((bucket) => (
                  <button
                    key={bucket}
                    type="button"
                    onClick={() => onToggleBucket(bucket)}
                    className={cn(
                      "border px-2 py-1 text-xs transition-colors",
                      buckets.includes(bucket)
                        ? "border-primary text-primary"
                        : "border-border text-muted-foreground hover:text-foreground",
                    )}
                  >
                    {MACHINE_BUCKET_LABELS[bucket]}
                  </button>
                ))}
              </div>

              {availableTags.length > 0 ? (
                <>
                  <p className="mb-2 mt-4 text-xs font-medium uppercase tracking-wide text-muted-foreground">
                    Tags
                  </p>
                  <div className="flex flex-wrap gap-1.5">
                    {availableTags.map((tag) => (
                      <button
                        key={tag}
                        type="button"
                        onClick={() => onToggleTag(tag)}
                        className={cn(
                          "border px-2 py-1 text-xs transition-colors",
                          tags.includes(tag)
                            ? "border-primary text-primary"
                            : "border-border text-muted-foreground hover:text-foreground",
                        )}
                      >
                        {tag}
                      </button>
                    ))}
                  </div>
                </>
              ) : null}

              {activeFilterCount > 0 ? (
                <button
                  type="button"
                  onClick={onClearFilters}
                  className="mt-4 text-xs text-muted-foreground underline underline-offset-2 hover:text-foreground"
                >
                  Clear filters
                </button>
              ) : null}
            </div>
          </PopoverContent>
        </Popover>
      </div>

      {/* Sort */}
      <div className="px-3 pb-2">
        <Select value={sortMode} onValueChange={(value) => onSortModeChange(value as SortMode)}>
          <SelectTrigger className="h-9">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {SORT_OPTIONS.map((option) => (
              <SelectItem key={option.value} value={option.value}>
                {option.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Virtualized list */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto px-3 pb-3">
        {clips.length === 0 ? (
          <p className="px-1 py-8 text-center text-sm text-muted-foreground">
            No clips match the current filters.
          </p>
        ) : (
          <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
            {virtualizer.getVirtualItems().map((virtualRow) => {
              const clip = clips[virtualRow.index]!;
              const isActive = clip.id === activeClipId;
              return (
                <div
                  key={clip.id}
                  data-index={virtualRow.index}
                  ref={virtualizer.measureElement}
                  style={{
                    position: "absolute",
                    top: 0,
                    left: 0,
                    width: "100%",
                    transform: `translateY(${virtualRow.start}px)`,
                  }}
                >
                  <button
                    type="button"
                    onClick={() => onSelect(clip.id)}
                    className={cn(
                      "mb-2 flex w-full flex-col gap-2 border p-3 text-left transition-colors",
                      isActive
                        ? "border-primary bg-secondary/50"
                        : "border-border hover:bg-secondary/30",
                    )}
                  >
                    <div className="flex items-center justify-between">
                      <span className="text-xs font-medium tabular-nums text-muted-foreground">
                        {clip.order}.
                      </span>
                      <StatusBadge status={clip.status} />
                    </div>
                    <p className="line-clamp-3 text-sm leading-snug">{clip.transcript}</p>
                    <div className="flex items-center justify-between text-xs text-muted-foreground">
                      <span className="tabular-nums">{formatSeconds(clip.durationSeconds)}</span>
                      <span>{clip.variant}</span>
                    </div>
                  </button>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </aside>
  );
}
