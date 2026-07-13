"use client";

import { Badge } from "@midday/ui/badge";
import { Button } from "@midday/ui/button";
import { cn } from "@midday/ui/cn";
import { Progress } from "@midday/ui/progress";
import { Separator } from "@midday/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@midday/ui/tabs";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@midday/ui/tooltip";
import {
  type LabClip,
  type ReviewStatus,
  MACHINE_BUCKET_LABELS,
  REASON_CODE_LABELS,
  REVIEW_STATUS_ORDER,
  STATUS_LABELS,
  formatDurationCompact,
  formatSeconds,
} from "./lab-data";
import { StatusBadge } from "./status-badge";

type Stats = {
  total: number;
  reviewed: number;
  counts: Record<ReviewStatus, number>;
};

type InspectorRailProps = {
  clip: LabClip;
  stats: Stats;
  onStatusChange: (status: ReviewStatus) => void;
  onSaveReference: () => void;
};

function StatRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between py-1 text-sm">
      <span className="text-muted-foreground">{label}</span>
      <span className="tabular-nums">{value}</span>
    </div>
  );
}

export function InspectorRail({
  clip,
  stats,
  onStatusChange,
  onSaveReference,
}: InspectorRailProps) {
  const reviewedPercent = stats.total > 0 ? (stats.reviewed / stats.total) * 100 : 0;

  return (
    <TooltipProvider delayDuration={100}>
      <aside className="flex h-full w-[340px] flex-shrink-0 flex-col overflow-y-auto border-l border-border">
        <div className="border-b border-border p-4">
          <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Inspector
          </p>
          <h2 className="mt-0.5 font-serif text-lg">Clip Review</h2>
        </div>

        {/* Always visible: live status + control */}
        <div className="space-y-4 border-b border-border p-4">
          <div className="flex items-center justify-between">
            <span className="text-xs uppercase tracking-wide text-muted-foreground">
              Live status
            </span>
            <StatusBadge status={clip.status} />
          </div>

          <div className="grid grid-cols-2 gap-1.5">
            {REVIEW_STATUS_ORDER.map((status) => (
              <Button
                key={status}
                type="button"
                size="sm"
                variant={clip.status === status ? "default" : "outline"}
                className="h-8 justify-center text-xs"
                onClick={() => onStatusChange(status)}
              >
                {STATUS_LABELS[status]}
              </Button>
            ))}
          </div>

          {/* Snapshot machine bucket — advisory, visually distinct from live */}
          <div className="flex items-center justify-between">
            <span className="text-xs uppercase tracking-wide text-muted-foreground">
              Machine QC
            </span>
            <Tooltip>
              <TooltipTrigger asChild>
                <Badge variant="outline" className="cursor-default gap-1">
                  {MACHINE_BUCKET_LABELS[clip.machineBucket]} · {clip.qcScore.toFixed(2)}
                </Badge>
              </TooltipTrigger>
              <TooltipContent side="left" className="max-w-56">
                <p className="text-xs">
                  Advisory machine triage from QC — a snapshot, not a decision. Your
                  live status above is the authoritative truth.
                </p>
              </TooltipContent>
            </Tooltip>
          </div>

          {clip.reasonCodes.length > 0 ? (
            <div className="flex flex-wrap gap-1.5">
              {clip.reasonCodes.map((code) => (
                <Tooltip key={code}>
                  <TooltipTrigger asChild>
                    <Badge variant="outline" className="cursor-default text-destructive">
                      {REASON_CODE_LABELS[code] ?? code}
                    </Badge>
                  </TooltipTrigger>
                  <TooltipContent side="left" className="max-w-56">
                    <p className="text-xs">
                      QC flagged this clip: {REASON_CODE_LABELS[code] ?? code}.
                    </p>
                  </TooltipContent>
                </Tooltip>
              ))}
            </div>
          ) : null}
        </div>

        {/* Always visible: compact stats + progress */}
        <div className="space-y-3 border-b border-border p-4">
          <div>
            <StatRow label="Total" value={String(stats.total)} />
            {REVIEW_STATUS_ORDER.filter((s) => stats.counts[s] > 0).map((status) => (
              <StatRow
                key={status}
                label={STATUS_LABELS[status]}
                value={String(stats.counts[status])}
              />
            ))}
          </div>
          <Separator />
          <div>
            <div className="mb-1.5 flex items-center justify-between text-sm">
              <span className="text-muted-foreground">Review progress</span>
              <span className="tabular-nums">{Math.round(reviewedPercent)}%</span>
            </div>
            <Progress value={reviewedPercent} className="h-2" />
          </div>
        </div>

        {/* Deep tooling behind tabs */}
        <div className="p-4">
          <Tabs defaultValue="history">
            <TabsList className="grid w-full grid-cols-4">
              <TabsTrigger value="history">History</TabsTrigger>
              <TabsTrigger value="edits">Edits</TabsTrigger>
              <TabsTrigger value="provenance">Source</TabsTrigger>
              <TabsTrigger value="reference">Ref</TabsTrigger>
            </TabsList>

            <TabsContent value="history" className="space-y-2">
              {clip.revisions.length > 0 ? (
                [...clip.revisions].reverse().map((rev) => (
                  <div key={rev.id} className="border border-border p-3">
                    <div className="flex items-center justify-between">
                      <span className="text-sm font-medium">{rev.message}</span>
                      <span className="text-xs text-muted-foreground">
                        {rev.milestone ? "milestone" : "auto"}
                      </span>
                    </div>
                    <p className="mt-1 line-clamp-2 text-xs text-muted-foreground">
                      {rev.transcript || "(blank transcript)"}
                    </p>
                    <span className="mt-1 block text-[11px] tabular-nums text-muted-foreground">
                      {new Date(rev.createdAt).toLocaleString()}
                    </span>
                  </div>
                ))
              ) : (
                <p className="py-4 text-sm text-muted-foreground">No saved history yet.</p>
              )}
            </TabsContent>

            <TabsContent value="edits" className="space-y-2">
              {clip.edits.length > 0 ? (
                clip.edits.map((edit, index) => (
                  <div
                    key={`${edit.op}-${index}`}
                    className="flex items-center justify-between border border-border p-3 text-sm"
                  >
                    <span className="font-medium">{edit.op.replace(/_/g, " ")}</span>
                    <span className="text-xs tabular-nums text-muted-foreground">
                      {edit.startSeconds !== undefined
                        ? edit.endSeconds !== undefined
                          ? `${formatSeconds(edit.startSeconds)}–${formatSeconds(edit.endSeconds)}`
                          : formatSeconds(edit.startSeconds)
                        : edit.durationSeconds !== undefined
                          ? formatSeconds(edit.durationSeconds)
                          : ""}
                    </span>
                  </div>
                ))
              ) : (
                <p className="py-4 text-sm text-muted-foreground">
                  No waveform edits on this clip yet.
                </p>
              )}
            </TabsContent>

            <TabsContent value="provenance" className="space-y-2">
              <StatRow label="Source" value={clip.sourceRecording} />
              <StatRow label="Speaker" value={clip.speaker} />
              <StatRow label="Language" value={clip.language} />
              <StatRow
                label="Original range"
                value={`${formatSeconds(clip.originalStartSeconds)}–${formatSeconds(clip.originalEndSeconds)}`}
              />
              <StatRow label="Duration" value={formatDurationCompact(clip.durationSeconds)} />
            </TabsContent>

            <TabsContent value="reference" className="space-y-3">
              <p className="text-sm text-muted-foreground">
                Save the current rendered slice — including active edits — into the
                reference library.
              </p>
              <Button
                type="button"
                variant="outline"
                className="w-full"
                onClick={onSaveReference}
              >
                Save current slice state
              </Button>
            </TabsContent>
          </Tabs>
        </div>
      </aside>
    </TooltipProvider>
  );
}
