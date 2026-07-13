"use client";

import { Badge } from "@midday/ui/badge";
import { Button } from "@midday/ui/button";
import { cn } from "@midday/ui/cn";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@midday/ui/collapsible";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@midday/ui/sheet";
import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { fetchRunLog } from "./speechcraft-api";

// Kind-A (process) observability. One run-scoped drawer: run status + the raw
// backend log, shown fully. Auto-surfaces on failure. The optional `stages`
// prop is for the wizard, where each step is a stage with its own status/log
// (current expanded, prior collapsed); the Lab omits it and just shows the
// run log. Backend run-log is a flat tail blob, so per-stage log text only
// exists when a caller supplies it (e.g. per-ProcessingJob in the wizard).

export type DiagnosticStage = {
  key: string;
  label: string;
  status: "pending" | "running" | "completed" | "failed" | "idle" | string;
  logText?: string;
};

const STATUS_TONE: Record<string, string> = {
  running: "text-blue-500 border-blue-500/40",
  completed: "text-emerald-600 border-emerald-600/40",
  failed: "text-red-500 border-red-500/40",
  needs_review: "text-amber-500 border-amber-500/40",
  pending: "text-muted-foreground",
  idle: "text-muted-foreground",
};

function LogPane({ text, truncated }: { text: string; truncated?: boolean }) {
  return (
    <div className="flex-1 overflow-auto bg-secondary/30 p-3">
      {truncated ? (
        <p className="mb-2 text-[11px] text-muted-foreground">
          Showing the latest portion of the log (truncated).
        </p>
      ) : null}
      <pre className="whitespace-pre-wrap break-words font-mono text-[11px] leading-relaxed text-foreground">
        {text || "(empty)"}
      </pre>
    </div>
  );
}

function StageRow({ stage }: { stage: DiagnosticStage }) {
  const [open, setOpen] = useState(
    stage.status === "running" || stage.status === "failed",
  );
  return (
    <Collapsible open={open} onOpenChange={setOpen} className="border-b border-border">
      <CollapsibleTrigger asChild>
        <button
          type="button"
          className="flex w-full items-center justify-between px-4 py-2.5 text-left hover:bg-secondary/40"
        >
          <span className="text-sm">{stage.label}</span>
          <Badge
            variant="outline"
            className={cn("text-[10px]", STATUS_TONE[stage.status])}
          >
            {stage.status}
          </Badge>
        </button>
      </CollapsibleTrigger>
      <CollapsibleContent>
        {stage.logText ? (
          <pre className="max-h-64 overflow-auto whitespace-pre-wrap break-words bg-secondary/30 px-4 py-2 font-mono text-[11px] leading-relaxed">
            {stage.logText}
          </pre>
        ) : (
          <p className="px-4 py-2 text-[11px] text-muted-foreground">
            No stage-specific log — see the full backend log below.
          </p>
        )}
      </CollapsibleContent>
    </Collapsible>
  );
}

export function DiagnosticsSheet({
  runId,
  runStatus,
  runStage,
  stages,
  triggerClassName,
}: {
  runId: string | null;
  runStatus?: string;
  runStage?: string;
  stages?: DiagnosticStage[];
  triggerClassName?: string;
}) {
  const [open, setOpen] = useState(false);
  const [autoOpenedFor, setAutoOpenedFor] = useState<string | null>(null);

  // Break-glass: surface automatically the first time a run fails.
  useEffect(() => {
    if (runStatus === "failed" && runId && autoOpenedFor !== runId) {
      setOpen(true);
      setAutoOpenedFor(runId);
    }
  }, [runStatus, runId, autoOpenedFor]);

  const {
    data: log,
    isLoading,
    refetch,
    isFetching,
  } = useQuery({
    queryKey: ["sc-runlog", runId],
    queryFn: () => fetchRunLog(runId!),
    enabled: open && !!runId,
    refetchInterval: open && runStatus === "running" ? 4000 : false,
  });

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger asChild>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className={cn("h-8 gap-1.5", triggerClassName)}
        >
          Diagnostics
          {runStatus === "failed" ? (
            <span className="size-1.5 rounded-full bg-red-500" aria-hidden />
          ) : null}
        </Button>
      </SheetTrigger>
      <SheetContent
        side="right"
        className="flex w-full flex-col gap-0 p-0 sm:max-w-[560px]"
      >
        <SheetHeader className="space-y-1.5 border-b border-border p-4 text-left">
          <SheetTitle className="font-serif text-base">Diagnostics</SheetTitle>
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <span className="font-mono">{runId ?? "—"}</span>
            {runStage ? (
              <>
                <span aria-hidden>·</span>
                <span>{runStage.replace(/_/g, " ")}</span>
              </>
            ) : null}
            {runStatus ? (
              <Badge
                variant="outline"
                className={cn("ml-auto text-[10px]", STATUS_TONE[runStatus])}
              >
                {runStatus}
              </Badge>
            ) : null}
          </div>
        </SheetHeader>

        {stages && stages.length > 0 ? (
          <div>{stages.map((s) => <StageRow key={s.key} stage={s} />)}</div>
        ) : null}

        <div className="flex items-center justify-between border-b border-border px-4 py-2">
          <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Backend log
          </span>
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="h-7"
            onClick={() => refetch()}
            disabled={!runId || isFetching}
          >
            {isFetching ? "Refreshing…" : "Refresh"}
          </Button>
        </div>

        {!runId ? (
          <p className="p-4 text-sm text-muted-foreground">No active run.</p>
        ) : isLoading ? (
          <p className="p-4 text-sm text-muted-foreground">Loading log…</p>
        ) : (
          <LogPane text={log?.text ?? ""} truncated={log?.truncated} />
        )}
      </SheetContent>
    </Sheet>
  );
}
