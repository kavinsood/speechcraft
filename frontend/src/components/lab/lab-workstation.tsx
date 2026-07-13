"use client";

import { cn } from "@midday/ui/cn";
import { Button } from "@midday/ui/button";
import { Icons } from "@midday/ui/icons";
import { useToast } from "@midday/ui/use-toast";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { ClipLabPanel } from "./clip-lab-panel";
import { ClipQueue } from "./clip-queue";
import { DatasetHealthPage } from "./dataset-health-page";
import { DiagnosticsSheet } from "./diagnostics-drawer";
import { ExportDialog } from "./export-dialog";
import { InspectorRail } from "./inspector-rail";
import { KeyboardBar } from "./keyboard-bar";
import { ProjectPicker } from "./project-picker";
import { LabRerunDialog } from "./rerun-dialog";
import { TranscriptPanel } from "./transcript-panel";
import {
  type ClipEdit,
  type LabClip,
  type MachineBucket,
  type ReviewStatus,
  type SortMode,
  createMockClips,
  filterClips,
  sortClips,
} from "./lab-data";
import { demoEnabled } from "@/lib/demo";
import {
  effectiveQcThresholds,
  fetchClipLabView,
  fetchDatasetQc,
  fetchDatasetRuns,
  fetchProjects,
  mapApiClip,
  pickReviewableRun,
} from "./speechcraft-api";
import {
  type DatasetAudioEditOperation,
  type DatasetClipLabClipView,
  SpeechcraftApiError,
  appendAudioOperation,
  markReferenceClipCandidate,
  patchClipLab,
  redoAudioOperation,
  undoAudioOperation,
} from "./speechcraft-write-api";

const PRESET_TAGS = [
  "mispronunciation",
  "background noise",
  "clipping",
  "overlap",
  "filler word",
  "breath",
];

type Mode = "qc" | "lab";

/** Optimistic-concurrency tokens the backend requires on every clip write. */
function tokensFor(clip: LabClip) {
  return {
    expected_manifest_sha256: clip.manifestSha as string,
    expected_clip_version: clip.clipVersion as number,
  };
}

const isLiveBacked = (clip: LabClip) =>
  clip.manifestSha != null && clip.clipVersion != null;

/** Fold an authoritative server clip view back into the local working copy. */
function mergeServerClip(prev: LabClip, s: DatasetClipLabClipView): LabClip {
  return {
    ...prev,
    status: s.review_status,
    transcript: s.transcript_override ?? s.transcript ?? prev.transcript,
    originalTranscript: s.original_transcript ?? prev.originalTranscript,
    tags: s.reviewer_tags ?? prev.tags,
    durationSeconds: s.current_duration_sec ?? prev.durationSeconds,
    transcriptConfidence: s.transcript_match ?? prev.transcriptConfidence,
    speakerPurity: s.speaker_check ?? prev.speakerPurity,
    reasonCodes: (s.pipeline_findings ?? []).map((f) => f.code),
    variant: s.effective_audio_kind ?? prev.variant,
    clipVersion: s.clip_version,
    effectiveAudioRevisionKey: s.effective_audio_revision_key,
    renderStatus: s.render_status,
    canUndoAudio: s.can_undo_audio,
    canRedoAudio: s.can_redo_audio,
    audioEditOpCount: s.audio_edit_op_count,
  };
}

export function LabWorkstation() {
  const searchParams = useSearchParams();
  const { toast, update } = useToast();
  const queryClient = useQueryClient();
  const demo = demoEnabled(searchParams);

  // ── Real data chain: projects → dataset runs → clip-lab view ──
  const { data: projects = [] } = useQuery({
    queryKey: ["sc-projects"],
    queryFn: fetchProjects,
    staleTime: 60_000,
    enabled: !demo,
  });

  const paramProjectId = searchParams.get("project");
  const activeProject =
    projects.find((p) => p.id === paramProjectId) ?? projects[0] ?? null;
  const projectId = activeProject?.id ?? null;

  const { data: runs = [], isLoading: runsLoading } = useQuery({
    queryKey: ["sc-runs", projectId],
    queryFn: () => fetchDatasetRuns(projectId!),
    enabled: !demo && !!projectId,
    staleTime: 60_000,
  });

  const run = useMemo(() => pickReviewableRun(runs), [runs]);
  const runId = run?.id ?? null;

  const {
    data: view,
    isLoading: viewLoading,
    error: viewError,
  } = useQuery({
    queryKey: ["sc-cliplab", runId],
    queryFn: () => fetchClipLabView(runId!),
    enabled: !demo && !!runId,
    staleTime: Number.POSITIVE_INFINITY,
  });

  // Committed (or default) QC thresholds — drives machineBucket below so
  // "auto_kept" in Clip Lab means whatever Dataset Health has committed,
  // not a hardcoded guess. Refetched whenever Dataset Health commits.
  const { data: qcPayload } = useQuery({
    queryKey: ["sc-dataset-qc", runId],
    queryFn: () => fetchDatasetQc(runId!),
    enabled: !demo && !!runId,
    staleTime: 30_000,
  });
  const qcThresholds = useMemo(() => effectiveQcThresholds(qcPayload), [qcPayload]);

  // ── Local (optimistic) working copy, seeded per run ──
  const [mode, setMode] = useState<Mode>("lab");
  const [exportOpen, setExportOpen] = useState(false);
  const [clips, setClips] = useState<LabClip[]>([]);
  const [activeClipId, setActiveClipId] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [filterStatuses, setFilterStatuses] = useState<ReviewStatus[]>([]);
  const [filterTags, setFilterTags] = useState<string[]>([]);
  const [sortMode, setSortMode] = useState<SortMode>("source");
  const [filterBuckets, setFilterBuckets] = useState<MachineBucket[]>([]);
  const seededRunRef = useRef<string | null>(null);

  useEffect(() => {
    if (!demo && view && runId && seededRunRef.current !== runId) {
      const mapped = view.clips.map((clip, index) =>
        mapApiClip(clip, index, runId, view.candidate_manifest_sha256, qcThresholds),
      );
      setClips(mapped);
      setActiveClipId(mapped[0]?.id ?? null);
      setSearch("");
      setFilterStatuses([]);
      setFilterTags([]);
      setFilterBuckets([]);
      seededRunRef.current = runId;
    }
    // qcThresholds intentionally excluded: this effect only seeds once per
    // run. Threshold changes after seeding are handled by the re-derive
    // effect below so in-progress edits aren't clobbered.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [view, runId, demo]);

  // Committed thresholds can change after clips are already seeded (Dataset
  // Health commits mid-session). Re-derive machineBucket/qcScore in place —
  // never touch review status, transcript, or audio edit state.
  useEffect(() => {
    if (demo) return;
    setClips((prev) =>
      prev.map((clip) => {
        const scoresKnown = clip.transcriptMatchRaw != null && clip.speakerCheckRaw != null;
        const hardFailed = clip.reasonCodes.length > 0;
        const passesGate =
          scoresKnown &&
          (clip.transcriptMatchRaw as number) >= qcThresholds.transcriptMatchMin &&
          (clip.speakerCheckRaw as number) >= qcThresholds.speakerCheckMin;
        const machineBucket: MachineBucket = !scoresKnown
          ? "needs_review"
          : hardFailed || !passesGate
            ? "auto_rejected"
            : "auto_kept";
        const qcScore = scoresKnown
          ? Math.max(
              0,
              Math.min(
                1,
                Math.min(
                  (clip.transcriptMatchRaw as number) / qcThresholds.transcriptMatchMin,
                  (clip.speakerCheckRaw as number) / qcThresholds.speakerCheckMin,
                ),
              ),
            )
          : 0;
        if (clip.machineBucket === machineBucket && clip.qcScore === qcScore) return clip;
        return { ...clip, machineBucket, qcScore: Number(qcScore.toFixed(3)) };
      }),
    );
  }, [qcThresholds, demo]);

  // Demo / UI-review mode: seed from mock clips, no backend. Writes stay local
  // (mock clips have no manifestSha/clipVersion, so applyClipWrite is a no-op
  // beyond the optimistic update).
  useEffect(() => {
    if (demo && seededRunRef.current !== "demo") {
      const mock = createMockClips();
      setClips(mock);
      setActiveClipId(mock[0]?.id ?? null);
      seededRunRef.current = "demo";
    }
  }, [demo]);

  const visibleClips = useMemo(
    () => sortClips(filterClips(clips, search, filterStatuses, filterTags, filterBuckets), sortMode),
    [clips, search, filterStatuses, filterTags, filterBuckets, sortMode],
  );

  const activeClip = useMemo(
    () => clips.find((c) => c.id === activeClipId) ?? null,
    [clips, activeClipId],
  );

  const availableTags = useMemo(() => {
    const set = new Set<string>();
    for (const clip of clips) for (const tag of clip.tags) set.add(tag);
    return Array.from(set).sort();
  }, [clips]);

  const allTags = useMemo(
    () => Array.from(new Set([...PRESET_TAGS, ...availableTags])),
    [availableTags],
  );

  const stats = useMemo(() => {
    const counts: Record<ReviewStatus, number> = {
      unresolved: 0,
      accepted: 0,
      rejected: 0,
      quarantined: 0,
    };
    for (const clip of clips) counts[clip.status] += 1;
    return { total: clips.length, reviewed: clips.length - counts.unresolved, counts };
  }, [clips]);

  const updateClip = useCallback((id: string, fn: (clip: LabClip) => LabClip) => {
    setClips((prev) => prev.map((clip) => (clip.id === id ? fn(clip) : clip)));
  }, []);

  // Refetch the authoritative clip-lab view and reseed (used after a 409 stale).
  const reseedFromServer = useCallback(async () => {
    if (!runId) return;
    const fresh = await fetchClipLabView(runId);
    const mapped = fresh.clips.map((clip, index) =>
      mapApiClip(clip, index, runId, fresh.candidate_manifest_sha256),
    );
    queryClient.setQueryData(["sc-cliplab", runId], fresh);
    setClips(mapped);
  }, [runId, queryClient]);

  // Optimistic write: apply locally, call backend, reconcile with the returned
  // authoritative clip (crucially, its new clip_version). On a stale 409,
  // reload the view; on any other failure, revert and surface the error.
  const applyClipWrite = useCallback(
    async (
      clipId: string,
      optimistic: (clip: LabClip) => LabClip,
      call: (clip: LabClip) => Promise<DatasetClipLabClipView>,
      label: string,
    ) => {
      const snapshot = clips.find((c) => c.id === clipId);
      if (!snapshot) return;
      updateClip(clipId, optimistic);
      if (!isLiveBacked(snapshot)) return; // mock / not-yet-live: local only
      try {
        const server = await call(snapshot);
        updateClip(clipId, (c) => mergeServerClip(c, server));
      } catch (err) {
        if (err instanceof SpeechcraftApiError && err.isStale) {
          toast({
            title: "Clip changed elsewhere",
            description: "Reloaded the latest version — reapply your change.",
            variant: "error",
            duration: 3500,
          });
          await reseedFromServer();
          return;
        }
        updateClip(clipId, () => snapshot); // revert
        toast({
          title: `${label} failed`,
          description:
            err instanceof SpeechcraftApiError ? err.detail : String(err),
          variant: "error",
          duration: 4000,
        });
      }
    },
    [clips, updateClip, toast, reseedFromServer],
  );

  const nextClipIdRef = useRef<string | null>(null);
  nextClipIdRef.current = (() => {
    if (!activeClipId) return null;
    const idx = visibleClips.findIndex((c) => c.id === activeClipId);
    return idx >= 0 ? (visibleClips[idx + 1]?.id ?? null) : null;
  })();

  const decide = useCallback(
    (status: ReviewStatus) => {
      if (!activeClipId) return;
      const nextId = nextClipIdRef.current;
      void applyClipWrite(
        activeClipId,
        (clip) => ({
          ...clip,
          status,
          revisions: [
            ...clip.revisions,
            {
              id: `${clip.id}-rev-${clip.revisions.length}`,
              message:
                status === "accepted"
                  ? "Accepted milestone"
                  : status === "rejected"
                    ? "Rejected milestone"
                    : `${status} milestone`,
              status,
              transcript: clip.transcript,
              createdAt: new Date().toISOString(),
              milestone: true,
            },
          ],
        }),
        (clip) =>
          patchClipLab(runId!, clip.id, {
            ...tokensFor(clip),
            review_status: status,
          }),
        "Status update",
      );
      if (nextId) setActiveClipId(nextId);
    },
    [activeClipId, applyClipWrite, runId],
  );

  const commitStatus = useCallback(
    (status: ReviewStatus) => {
      if (!activeClipId) return;
      void applyClipWrite(
        activeClipId,
        (clip) => ({ ...clip, status }),
        (clip) =>
          patchClipLab(runId!, clip.id, { ...tokensFor(clip), review_status: status }),
        "Status update",
      );
    },
    [activeClipId, applyClipWrite, runId],
  );

  const commitTranscript = useCallback(
    (text: string) => {
      if (!activeClipId) return;
      void applyClipWrite(
        activeClipId,
        (clip) => ({ ...clip, transcript: text }),
        (clip) =>
          patchClipLab(runId!, clip.id, {
            ...tokensFor(clip),
            transcript_override: text,
          }),
        "Transcript save",
      );
    },
    [activeClipId, applyClipWrite, runId],
  );

  const commitTags = useCallback(
    (tags: string[]) => {
      if (!activeClipId) return;
      void applyClipWrite(
        activeClipId,
        (clip) => ({ ...clip, tags }),
        (clip) =>
          patchClipLab(runId!, clip.id, { ...tokensFor(clip), reviewer_tags: tags }),
        "Tag update",
      );
    },
    [activeClipId, applyClipWrite, runId],
  );

  const navigate = useCallback(
    (direction: "prev" | "next") => {
      if (!activeClipId) return;
      const idx = visibleClips.findIndex((c) => c.id === activeClipId);
      if (idx === -1) return;
      const target = visibleClips[idx + (direction === "next" ? 1 : -1)];
      if (target) setActiveClipId(target.id);
    },
    [activeClipId, visibleClips],
  );

  const appendEdit = useCallback(
    (edit: ClipEdit) => {
      if (!activeClipId) return;
      const clip = clips.find((c) => c.id === activeClipId);
      if (!clip) return;
      const sr = clip.sampleRateHz || 16000;
      let operation: DatasetAudioEditOperation | null = null;
      if (edit.op === "delete_range" && edit.startSeconds != null && edit.endSeconds != null) {
        operation = {
          kind: "delete_range",
          start_sample: Math.round(edit.startSeconds * sr),
          end_sample: Math.round(edit.endSeconds * sr),
        };
      } else if (
        edit.op === "insert_silence" &&
        edit.startSeconds != null &&
        edit.durationSeconds != null
      ) {
        operation = {
          kind: "insert_silence",
          at_sample: Math.round(edit.startSeconds * sr),
          duration_samples: Math.round(edit.durationSeconds * sr),
        };
      }
      if (!operation) {
        // split / merge_next are not backend audio operations.
        toast({
          title: "Not supported yet",
          description: `"${edit.op}" isn't a backend audio operation.`,
          variant: "error",
          duration: 3000,
        });
        return;
      }
      const backendOp = operation;
      void applyClipWrite(
        activeClipId,
        (c) => ({ ...c, edits: [...c.edits, edit] }),
        (c) => appendAudioOperation(runId!, c.id, { ...tokensFor(c), operation: backendOp }),
        "Audio edit",
      );
    },
    [activeClipId, clips, applyClipWrite, runId, toast],
  );

  const undoEdit = useCallback(() => {
    if (!activeClipId) return;
    void applyClipWrite(
      activeClipId,
      (c) => c,
      (c) => undoAudioOperation(runId!, c.id, tokensFor(c)),
      "Undo",
    );
  }, [activeClipId, applyClipWrite, runId]);

  const redoEdit = useCallback(() => {
    if (!activeClipId) return;
    void applyClipWrite(
      activeClipId,
      (c) => c,
      (c) => redoAudioOperation(runId!, c.id, tokensFor(c)),
      "Redo",
    );
  }, [activeClipId, applyClipWrite, runId]);

  const markReference = useCallback(() => {
    const clip = clips.find((c) => c.id === activeClipId);
    if (!clip || !projectId || !runId) return;
    void (async () => {
      try {
        await markReferenceClipCandidate(projectId, runId, {
          clip_id: clip.id,
          transcript_text: clip.transcript,
        });
        toast({ title: "Marked as reference candidate", variant: "success", duration: 2000 });
      } catch (err) {
        toast({
          title: "Mark reference failed",
          description: err instanceof SpeechcraftApiError ? err.detail : String(err),
          variant: "error",
          duration: 4000,
        });
      }
    })();
  }, [clips, activeClipId, projectId, runId, toast]);

  const runModel = useCallback(() => {
    if (!activeClip) return;
    const clipId = activeClip.id;
    const { id } = toast({
      title: "Running DeepFilterNet",
      description: "Enhancing slice audio…",
      variant: "progress",
      progress: 0,
      duration: Number.POSITIVE_INFINITY,
    });
    const started = Date.now();
    const interval = setInterval(() => {
      const pct = Math.min(100, Math.round(((Date.now() - started) / 3500) * 100));
      update(id, { id, progress: pct });
      if (pct >= 100) {
        clearInterval(interval);
        updateClip(clipId, (clip) => ({ ...clip, variant: "deepfilternet" }));
        update(id, {
          id,
          title: "Enhancement complete",
          description: "Activated the DeepFilterNet variant.",
          variant: "success",
          duration: 2500,
        });
      }
    }, 120);
  }, [activeClip, toast, update, updateClip]);

  const hotkeyEnabled = mode === "lab" && !!activeClipId;
  useHotkeys("enter", (e) => { e.preventDefault(); decide("accepted"); }, { enabled: hotkeyEnabled }, [decide]);
  useHotkeys("shift+enter", (e) => { e.preventDefault(); decide("rejected"); }, { enabled: hotkeyEnabled }, [decide]);
  useHotkeys("up", (e) => { e.preventDefault(); navigate("prev"); }, { enabled: hotkeyEnabled }, [navigate]);
  useHotkeys("down", (e) => { e.preventDefault(); navigate("next"); }, { enabled: hotkeyEnabled }, [navigate]);
  useHotkeys("meta+z", (e) => { e.preventDefault(); undoEdit(); }, { enabled: hotkeyEnabled }, [undoEdit]);
  useHotkeys("meta+shift+z", (e) => { e.preventDefault(); redoEdit(); }, { enabled: hotkeyEnabled }, [redoEdit]);

  const isLoading =
    !demo &&
    ((!projectId && projects.length === 0) || runsLoading || (viewLoading && clips.length === 0));

  const undoAvailable = activeClip
    ? isLiveBacked(activeClip)
      ? !!activeClip.canUndoAudio
      : activeClip.edits.length > 0
    : false;
  const redoAvailable = activeClip
    ? isLiveBacked(activeClip)
      ? !!activeClip.canRedoAudio
      : false
    : false;

  return (
    <div className="flex h-screen flex-col">
      <header className="flex h-[70px] flex-shrink-0 items-center justify-between border-b border-border px-6">
        <div className="flex items-center gap-4">
          <Link href="/" aria-label="Home" className="flex items-center text-foreground">
            <Icons.LogoSmall />
          </Link>
          <div className="inline-flex border border-border p-0.5">
            <button
              type="button"
              onClick={() => setMode("qc")}
              className={cn(
                "px-3 py-1.5 text-sm transition-colors",
                mode === "qc" ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground",
              )}
            >
              Dataset Health
            </button>
            <button
              type="button"
              onClick={() => setMode("lab")}
              className={cn(
                "px-3 py-1.5 text-sm transition-colors",
                mode === "lab" ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground",
              )}
            >
              Clip Lab
            </button>
          </div>
          <div>
            <h1 className="font-serif text-lg leading-none">
              {mode === "lab" ? "Clip Lab" : "Dataset Health"}
            </h1>
            <p className="mt-0.5 text-xs text-muted-foreground">
              {mode === "lab"
                ? "Manual slice review, transcript repair, and human overrides."
                : "Machine triage and yield tuning."}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-8 gap-1.5"
            onClick={() => setExportOpen(true)}
          >
            <Icons.Share className="size-4" />
            Export
          </Button>
          <LabRerunDialog projectId={projectId} runId={runId} />
          <DiagnosticsSheet
            runId={runId}
            runStatus={run?.status}
            runStage={run?.stage}
          />
          <ProjectPicker />
        </div>
      </header>

      <ExportDialog open={exportOpen} onOpenChange={setExportOpen} />

      {mode === "qc" ? (
        <DatasetHealthPage runId={runId} demo={demo} />
      ) : viewError ? (
        <div className="flex flex-1 items-center justify-center">
          <div className="max-w-sm text-center">
            <p className="font-serif text-xl">Backend unavailable</p>
            <p className="mt-2 text-sm text-muted-foreground">
              Couldn't reach the speechcraft backend. Make sure it's running on
              :8010 (make dev-backend), then reload.
            </p>
          </div>
        </div>
      ) : isLoading ? (
        <div className="flex flex-1 items-center justify-center text-sm text-muted-foreground">
          Loading clips…
        </div>
      ) : clips.length === 0 ? (
        <div className="flex flex-1 items-center justify-center text-sm text-muted-foreground">
          No candidate clips in this project's dataset run yet.
        </div>
      ) : (
        <div className="flex flex-1 overflow-hidden">
          <ClipQueue
            clips={visibleClips}
            activeClipId={activeClipId}
            search={search}
            onSearchChange={setSearch}
            statuses={filterStatuses}
            tags={filterTags}
            availableTags={availableTags}
            buckets={filterBuckets}
            onToggleStatus={(status) =>
              setFilterStatuses((cur) =>
                cur.includes(status) ? cur.filter((s) => s !== status) : [...cur, status],
              )
            }
            onToggleTag={(tag) =>
              setFilterTags((cur) =>
                cur.includes(tag) ? cur.filter((t) => t !== tag) : [...cur, tag],
              )
            }
            onToggleBucket={(bucket) =>
              setFilterBuckets((cur) =>
                cur.includes(bucket) ? cur.filter((b) => b !== bucket) : [...cur, bucket],
              )
            }
            onClearFilters={() => {
              setFilterStatuses([]);
              setFilterTags([]);
              setFilterBuckets([]);
            }}
            sortMode={sortMode}
            onSortModeChange={setSortMode}
            onSelect={setActiveClipId}
          />

          <div className="flex flex-1 flex-col overflow-hidden">
            <div className="flex-1 space-y-4 overflow-y-auto p-4">
              {activeClip ? (
                <>
                  <ClipLabPanel
                    clip={activeClip}
                    canUndo={undoAvailable}
                    canRedo={redoAvailable}
                    onAccept={() => decide("accepted")}
                    onReject={() => decide("rejected")}
                    onAppendEdit={appendEdit}
                    onUndo={undoEdit}
                    onRedo={redoEdit}
                    onMarkReference={markReference}
                    onRunModel={runModel}
                  />
                  <TranscriptPanel
                    clip={activeClip}
                    allTags={allTags}
                    onTranscriptChange={commitTranscript}
                    onTagsChange={commitTags}
                  />
                </>
              ) : (
                <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
                  Select a clip to begin review.
                </div>
              )}
            </div>
            <KeyboardBar />
          </div>

          {activeClip ? (
            <InspectorRail
              clip={activeClip}
              stats={stats}
              onStatusChange={commitStatus}
              onSaveReference={markReference}
            />
          ) : null}
        </div>
      )}
    </div>
  );
}
