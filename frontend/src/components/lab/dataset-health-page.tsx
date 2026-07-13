"use client";

import { Button } from "@midday/ui/button";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@midday/ui/alert-dialog";
import { useToast } from "@midday/ui/use-toast";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { BestRejectedTable, RiskiestKeptTable } from "./boundary-tables";
import { combinedSummary, histogram, unscoredCount, type QcClip } from "./qc-logic";
import { fetchDatasetQc, type DatasetQcClipApi } from "./speechcraft-api";
import { finalizeDatasetQc, SpeechcraftApiError } from "./speechcraft-write-api";
import { ThresholdHistogramChart } from "./threshold-histogram-chart";

function toQcClip(api: DatasetQcClipApi): QcClip {
  return {
    clipId: api.clip_id,
    durationSec: api.duration_sec,
    transcriptMatch: api.transcript_match,
    speakerCheck: api.speaker_check,
    reasonCodes: [
      ...api.transcript_reason_codes,
      ...api.speaker_reason_codes,
      ...api.candidate_reason_codes,
      ...api.qc_reason_codes,
    ],
    trainingText: api.training_text,
  };
}

function formatDuration(seconds: number): string {
  const mins = Math.floor(seconds / 60);
  const secs = Math.round(seconds % 60);
  return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
}

const DEMO_CLIPS: QcClip[] = Array.from({ length: 140 }, (_, i) => {
  const seed = (i * 9301 + 49297) % 233280;
  const rand = seed / 233280;
  const transcriptMatch = Math.max(0, Math.min(100, 60 + rand * 45 - 10));
  const speakerCheck = Math.max(0, Math.min(100, 55 + ((seed * 3) % 233280) / 233280 * 45));
  return {
    clipId: `demo-clip-${i + 1}`,
    durationSec: 3 + rand * 9,
    transcriptMatch: i % 17 === 0 ? null : Number(transcriptMatch.toFixed(1)),
    speakerCheck: i % 23 === 0 ? null : Number(speakerCheck.toFixed(1)),
    reasonCodes: [],
    trainingText: "The quick brown fox jumps over the lazy dog near the riverbank.",
  };
});

export function DatasetHealthPage({
  runId,
  demo = false,
}: {
  runId: string | null;
  demo?: boolean;
}) {
  const { toast } = useToast();
  const queryClient = useQueryClient();
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [committing, setCommitting] = useState(false);

  const { data, isLoading, error } = useQuery({
    queryKey: ["sc-dataset-qc", runId],
    queryFn: () => fetchDatasetQc(runId!),
    enabled: !demo && !!runId,
    staleTime: 30_000,
  });

  const clips = useMemo(
    () => (demo ? DEMO_CLIPS : (data?.clips ?? []).map(toQcClip)),
    [demo, data],
  );

  const defaults = data?.defaults ?? { transcript_match_threshold: 85, speaker_check_threshold: 70 };
  const initialTranscript = data?.finalized_thresholds?.transcript_match_min ?? defaults.transcript_match_threshold;
  const initialSpeaker = data?.finalized_thresholds?.speaker_check_min ?? defaults.speaker_check_threshold;

  const [transcriptThreshold, setTranscriptThreshold] = useState(initialTranscript);
  const [speakerThreshold, setSpeakerThreshold] = useState(initialSpeaker);

  // Re-sync local thresholds once when the finalized state actually loads
  // (demo mode has no async load, so it's already correct on first render).
  const syncKey = demo ? "demo" : data ? "loaded" : "pending";
  const [lastSyncKey, setLastSyncKey] = useState<string | null>(null);
  if (syncKey !== "pending" && syncKey !== lastSyncKey) {
    setTranscriptThreshold(initialTranscript);
    setSpeakerThreshold(initialSpeaker);
    setLastSyncKey(syncKey);
  }

  const transcriptBins = useMemo(() => histogram(clips, (c) => c.transcriptMatch), [clips]);
  const speakerBins = useMemo(() => histogram(clips, (c) => c.speakerCheck), [clips]);

  const transcriptAccepted = useMemo(
    () => clips.filter((c) => (c.transcriptMatch ?? -1) >= transcriptThreshold),
    [clips, transcriptThreshold],
  );
  const speakerAccepted = useMemo(
    () => clips.filter((c) => (c.speakerCheck ?? -1) >= speakerThreshold),
    [clips, speakerThreshold],
  );

  const summary = useMemo(
    () => combinedSummary(clips, transcriptThreshold, speakerThreshold),
    [clips, transcriptThreshold, speakerThreshold],
  );

  const ready = demo || (data?.ready ?? false);
  const qcNotReady = !demo && data && !data.ready;

  const handleCommit = async () => {
    if (!runId) return;
    setCommitting(true);
    try {
      const result = await finalizeDatasetQc(runId, {
        transcript_match_min: transcriptThreshold,
        speaker_check_min: speakerThreshold,
      });
      toast({
        title: "Thresholds committed",
        description: `${result.summary.accepted_count} clips accepted (${formatDuration(result.summary.accepted_duration_sec)}). Export is now stale.`,
        variant: "success",
        duration: 4000,
      });
      queryClient.invalidateQueries({ queryKey: ["sc-dataset-qc", runId] });
      queryClient.invalidateQueries({ queryKey: ["sc-cliplab", runId] });
    } catch (err) {
      toast({
        title: "Commit failed",
        description: err instanceof SpeechcraftApiError ? err.detail : String(err),
        variant: "error",
        duration: 4500,
      });
    } finally {
      setCommitting(false);
      setConfirmOpen(false);
    }
  };

  if (!demo && !runId) {
    return (
      <div className="flex flex-1 items-center justify-center">
        <p className="text-sm text-muted-foreground">No dataset run selected yet.</p>
      </div>
    );
  }

  if (!demo && isLoading) {
    return (
      <div className="flex flex-1 items-center justify-center text-sm text-muted-foreground">
        Loading dataset health…
      </div>
    );
  }

  if (!demo && error) {
    return (
      <div className="flex flex-1 items-center justify-center">
        <div className="max-w-sm text-center">
          <p className="font-serif text-xl">Couldn't load QC</p>
          <p className="mt-2 text-sm text-muted-foreground">
            {error instanceof SpeechcraftApiError ? error.detail : String(error)}
          </p>
        </div>
      </div>
    );
  }

  if (qcNotReady) {
    return (
      <div className="flex flex-1 items-center justify-center">
        <div className="max-w-sm text-center">
          <p className="font-serif text-xl">QC hasn't run yet</p>
          <p className="mt-2 text-sm text-muted-foreground">
            {data?.missing_artifacts.length
              ? `Missing: ${data.missing_artifacts.join(", ")}.`
              : "Run transcript and speaker QC for this run to see dataset health."}
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto">
      <div className="mx-auto max-w-[1400px] p-6">
        <div className="mb-6 flex items-center justify-between">
          <div className="text-sm text-muted-foreground">
            {summary.acceptedCount} of {clips.length} clips pass both gates at these thresholds ·{" "}
            {formatDuration(summary.acceptedDurationSec)}
            {data?.finalized && (
              <span className="ml-2 text-xs text-[#878787]">
                (committed: {data.finalized_thresholds?.transcript_match_min}/
                {data.finalized_thresholds?.speaker_check_min})
              </span>
            )}
          </div>
          <Button
            type="button"
            size="sm"
            className="h-8"
            disabled={demo || !runId || committing}
            onClick={() => setConfirmOpen(true)}
          >
            {committing ? "Committing…" : "Commit thresholds"}
          </Button>
        </div>

        <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
          <ThresholdHistogramChart
            title="Transcript match"
            subtitle="CTC-aligned minimum span score. Clips below the line fail this gate."
            bins={transcriptBins}
            threshold={transcriptThreshold}
            onThresholdChange={setTranscriptThreshold}
            unscoredCount={unscoredCount(clips, (c) => c.transcriptMatch)}
            acceptedCount={transcriptAccepted.length}
            acceptedDurationSec={transcriptAccepted.reduce((sum, c) => sum + c.durationSec, 0)}
          />
          <ThresholdHistogramChart
            title="Speaker check"
            subtitle="Minimum speaker-embedding window similarity. Clips below the line fail this gate."
            bins={speakerBins}
            threshold={speakerThreshold}
            onThresholdChange={setSpeakerThreshold}
            unscoredCount={unscoredCount(clips, (c) => c.speakerCheck)}
            acceptedCount={speakerAccepted.length}
            acceptedDurationSec={speakerAccepted.reduce((sum, c) => sum + c.durationSec, 0)}
          />
        </div>

        <div className="mt-6 grid grid-cols-1 gap-6 md:grid-cols-2">
          <RiskiestKeptTable
            clips={clips}
            transcriptThreshold={transcriptThreshold}
            speakerThreshold={speakerThreshold}
          />
          <BestRejectedTable
            clips={clips}
            transcriptThreshold={transcriptThreshold}
            speakerThreshold={speakerThreshold}
          />
        </div>
      </div>

      <AlertDialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="font-serif">Commit these thresholds?</AlertDialogTitle>
            <AlertDialogDescription>
              This writes the verdict for every clip at transcript ≥ {transcriptThreshold} and
              speaker ≥ {speakerThreshold}, and invalidates any existing export for this run.
              Human overrides in Clip Lab still take priority.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={committing}>Cancel</AlertDialogCancel>
            <AlertDialogAction disabled={committing} onClick={() => void handleCommit()}>
              {committing ? "Committing…" : "Commit"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
