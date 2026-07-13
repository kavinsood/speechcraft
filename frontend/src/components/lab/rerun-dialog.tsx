"use client";

import { Button } from "@midday/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@midday/ui/dialog";
import { Input } from "@midday/ui/input";
import { Label } from "@midday/ui/label";
import { Separator } from "@midday/ui/separator";
import { Textarea } from "@midday/ui/textarea";
import { useToast } from "@midday/ui/use-toast";
import { useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { useState } from "react";
import { SpeechcraftApiError } from "./speechcraft-write-api";
import {
  createDatasetRun,
  fetchRun,
  resolveWhisperModel,
  slicerRerun,
} from "@/components/wizard/wizard-api";

// The two downstream re-run boundaries from the design:
//  - cheap re-slice (adjust clip geometry) via slicer-rerun on the SAME run
//  - expensive re-run with externally-known overrides (initial_prompt /
//    text normalization) as a NEW run cloned from the current one.
// These live here in the loop — never as wizard knobs — because the insight
// to change them only arises after seeing results.

const SLICER_FIELDS: { key: string; label: string; unit: string; step: number }[] = [
  { key: "candidate_target_clip_sec", label: "Target clip", unit: "sec", step: 0.5 },
  { key: "candidate_min_clip_sec", label: "Minimum clip", unit: "sec", step: 0.5 },
  { key: "candidate_max_clip_sec", label: "Maximum clip", unit: "sec", step: 0.5 },
  { key: "cutpoint_min_gap_ms", label: "Smallest cuttable gap", unit: "ms", step: 5 },
  { key: "cutpoint_left_word_edge_guard_ms", label: "Left word-edge guard", unit: "ms", step: 5 },
  { key: "cutpoint_right_word_edge_guard_ms", label: "Right word-edge guard", unit: "ms", step: 5 },
];

const SLICER_DEFAULTS: Record<string, number> = {
  candidate_target_clip_sec: 8,
  candidate_min_clip_sec: 3,
  candidate_max_clip_sec: 15,
  cutpoint_min_gap_ms: 80,
  cutpoint_left_word_edge_guard_ms: 30,
  cutpoint_right_word_edge_guard_ms: 30,
};

const NORMALIZATION = ["strict", "loose", "spoken_form"] as const;

export function LabRerunDialog({
  projectId,
  runId,
}: {
  projectId: string | null;
  runId: string | null;
}) {
  const { toast } = useToast();
  const router = useRouter();
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [slicer, setSlicer] = useState<Record<string, number>>(SLICER_DEFAULTS);
  const [initialPrompt, setInitialPrompt] = useState("");
  const [textNorm, setTextNorm] = useState<(typeof NORMALIZATION)[number]>("loose");
  const [busy, setBusy] = useState<null | "reslice" | "rerun">(null);

  const reslice = async () => {
    if (!runId) return;
    setBusy("reslice");
    try {
      await slicerRerun(runId, slicer);
      toast({
        title: "Re-slicing started",
        description: "New candidate clips are being generated — reload when it completes.",
        variant: "success",
        duration: 3500,
      });
      queryClient.invalidateQueries({ queryKey: ["sc-cliplab", runId] });
      setOpen(false);
    } catch (err) {
      toast({
        title: "Re-slice failed",
        description: err instanceof SpeechcraftApiError ? err.detail : String(err),
        variant: "error",
        duration: 4000,
      });
    } finally {
      setBusy(null);
    }
  };

  const rerunAsr = async () => {
    if (!projectId || !runId) return;
    setBusy("rerun");
    try {
      const current = await fetchRun(runId);
      const sourceIds = (current.input_summary.source_recording_ids as string[]) ?? [];
      const singleSpeaker = Boolean(current.input_summary.single_speaker);
      const resolved = await resolveWhisperModel();
      if (resolved.model === null) throw new SpeechcraftApiError(0, resolved.error);
      const fresh = await createDatasetRun(projectId, {
        source_recording_ids: sourceIds,
        whisper_model_size: resolved.model,
        single_speaker: singleSpeaker,
        config: {
          initial_prompt: initialPrompt.trim() || undefined,
          text_normalization_strategy: textNorm,
        },
      });
      toast({ title: "New run created", description: "Re-running with your overrides.", variant: "success", duration: 2500 });
      setOpen(false);
      router.push(
        `/login4?project=${encodeURIComponent(projectId)}&run=${encodeURIComponent(fresh.id)}`,
      );
    } catch (err) {
      toast({
        title: "Re-run failed",
        description: err instanceof SpeechcraftApiError ? err.detail : String(err),
        variant: "error",
        duration: 4000,
      });
    } finally {
      setBusy(null);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button type="button" variant="outline" size="sm" className="h-8" disabled={!runId}>
          Re-run
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle className="font-serif">Re-run with adjustments</DialogTitle>
        </DialogHeader>

        <div className="space-y-4">
          <div>
            <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Re-slice (fast — same transcription)
            </p>
            <p className="mb-3 text-xs text-muted-foreground">
              Adjust clip geometry and regenerate candidate clips. Existing QC becomes stale.
            </p>
            <div className="grid grid-cols-2 gap-3">
              {SLICER_FIELDS.map((f) => (
                <label key={f.key} className="flex flex-col gap-1 text-xs">
                  <span className="text-muted-foreground">
                    {f.label} <span className="text-[10px]">({f.unit})</span>
                  </span>
                  <Input
                    type="number"
                    step={f.step}
                    value={slicer[f.key]}
                    onChange={(e) =>
                      setSlicer((s) => ({ ...s, [f.key]: Number(e.target.value) }))
                    }
                    className="h-8"
                  />
                </label>
              ))}
            </div>
            <div className="mt-3 flex justify-end">
              <Button
                type="button"
                size="sm"
                className="h-8"
                onClick={() => void reslice()}
                disabled={busy !== null}
              >
                {busy === "reslice" ? "Re-slicing…" : "Re-slice this run"}
              </Button>
            </div>
          </div>

          <Separator />

          <div>
            <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Re-transcribe (slow — new run)
            </p>
            <p className="mb-3 text-xs text-muted-foreground">
              For systematic transcription issues. Creates a fresh run with your overrides.
            </p>
            <label className="flex flex-col gap-1 text-xs">
              <span className="text-muted-foreground">Vocabulary hint (initial prompt)</span>
              <Textarea
                value={initialPrompt}
                onChange={(e) => setInitialPrompt(e.target.value)}
                placeholder="e.g. proper nouns / jargon the model keeps mishearing"
                className="min-h-[60px] text-sm"
              />
            </label>
            <label className="mt-3 flex flex-col gap-1 text-xs">
              <span className="text-muted-foreground">Text normalization</span>
              <select
                value={textNorm}
                onChange={(e) => setTextNorm(e.target.value as (typeof NORMALIZATION)[number])}
                className="h-8 border border-border bg-transparent px-2 text-sm"
              >
                {NORMALIZATION.map((n) => (
                  <option key={n} value={n}>{n}</option>
                ))}
              </select>
            </label>
          </div>
        </div>

        <DialogFooter>
          <Button
            type="button"
            variant="secondary"
            onClick={() => void rerunAsr()}
            disabled={busy !== null}
          >
            {busy === "rerun" ? "Creating run…" : "Re-transcribe (new run)"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
