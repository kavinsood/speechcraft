"use client";

import { cn } from "@midday/ui/cn";
import { Icons } from "@midday/ui/icons";
import { Spinner } from "@midday/ui/spinner";
import { SubmitButton } from "@midday/ui/submit-button";
import { useRouter, useSearchParams } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";
import { DiagnosticsSheet } from "@/components/lab/diagnostics-drawer";
import { SpeechcraftApiError } from "@/components/lab/speechcraft-write-api";
import { demoEnabled, withDemo } from "@/lib/demo";
import {
  type SpeakerSample,
  distinctSpeakerIds,
  fetchSpeakers,
  refreshRun,
  saveSpeakerSelection,
  speakerSampleUrl,
} from "./wizard/wizard-api";

type Phase = "detecting" | "select" | "error";

const DEMO_SPEAKERS: { id: string; sample: SpeakerSample }[] = [
  { id: "speaker_0", sample: { sample_id: "demo_0", speaker_id: "speaker_0", source_audio_id: "demo", audio_path: "", duration_sec: 5.4 } },
  { id: "speaker_1", sample: { sample_id: "demo_1", speaker_id: "speaker_1", source_audio_id: "demo", audio_path: "", duration_sec: 4.1 } },
];

export function VoiceSelection() {
  const router = useRouter();
  const params = useSearchParams();
  const projectId = params.get("project");
  const runId = params.get("run");
  const demo = demoEnabled(params);

  const [phase, setPhase] = useState<Phase>("detecting");
  const [error, setError] = useState<string | null>(null);
  const [runStatus, setRunStatus] = useState<string | undefined>();
  const [speakers, setSpeakers] = useState<{ id: string; sample: SpeakerSample }[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [playingId, setPlayingId] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const audioRef = useRef<HTMLAudioElement | null>(null);
  const startedRef = useRef(false);

  const goNext = useCallback(() => {
    router.push(
      withDemo(
        `/login5?project=${encodeURIComponent(projectId!)}&run=${encodeURIComponent(runId!)}`,
        demo,
      ),
    );
  }, [router, projectId, runId, demo]);

  // Poll until diarization produces speaker samples; branch on speaker count.
  useEffect(() => {
    if (startedRef.current) return;
    startedRef.current = true;
    if (!projectId || !runId) {
      setPhase("error");
      setError("Missing project/run in the URL. Restart from ingest.");
      return;
    }
    if (demo) {
      setSpeakers(DEMO_SPEAKERS);
      setPhase("select");
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        for (;;) {
          if (cancelled) return;
          const run = await refreshRun(runId);
          setRunStatus(run.status);
          if (run.status === "failed" || run.status === "rejected") {
            throw new SpeechcraftApiError(0, `Speaker detection ${run.status}.`);
          }
          const results = await fetchSpeakers(runId);
          if (results.speaker_samples_manifest.length > 0) {
            const ids = distinctSpeakerIds(results);
            // One sample per speaker for preview.
            const bySpeaker = ids.map((id) => ({
              id,
              sample: results.speaker_samples_manifest.find((s) => s.speaker_id === id)!,
            }));
            if (ids.length <= 1) {
              // Single speaker detected — no page, auto-select and continue.
              await saveSpeakerSelection(runId, ids[0] ?? "speaker_0");
              if (!cancelled) goNext();
              return;
            }
            if (!cancelled) {
              setSpeakers(bySpeaker);
              setPhase("select");
            }
            return;
          }
          await new Promise((r) => setTimeout(r, 2500));
        }
      } catch (err) {
        if (cancelled) return;
        setPhase("error");
        setError(err instanceof SpeechcraftApiError ? err.detail : "Speaker detection failed.");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [projectId, runId, goNext, demo]);

  const togglePlay = (speakerId: string, sampleId: string) => {
    const el = audioRef.current;
    if (!el || !runId) return;
    if (playingId === speakerId) {
      el.pause();
      setPlayingId(null);
      return;
    }
    el.src = speakerSampleUrl(runId, sampleId);
    void el.play();
    setPlayingId(speakerId);
  };

  const confirm = async () => {
    if (!selectedId || !runId) return;
    setBusy(true);
    try {
      if (!demo) await saveSpeakerSelection(runId, selectedId);
      goNext();
    } catch (err) {
      setBusy(false);
      setPhase("error");
      setError(err instanceof SpeechcraftApiError ? err.detail : "Could not save selection.");
    }
  };

  if (phase === "error") {
    return (
      <div className="text-center space-y-2">
        <h1 className="text-lg lg:text-xl mb-4 font-serif">Speaker detection failed</h1>
        <p className="font-sans text-sm text-[#878787]">{error}</p>
        <div className="pt-6">
          <DiagnosticsSheet runId={runId} runStatus={runStatus} runStage="diarization" />
        </div>
      </div>
    );
  }

  if (phase === "detecting") {
    return (
      <>
        <div className="text-center space-y-2">
          <h1 className="text-lg lg:text-xl mb-4 font-serif">Detecting speakers</h1>
          <p className="font-sans text-sm text-[#878787]">
            Finding every voice in your recordings…
          </p>
        </div>
        <div className="mt-10 lg:mt-12 flex items-center justify-center">
          <Spinner size={20} />
        </div>
      </>
    );
  }

  return (
    <>
      {/* Hidden shared audio element for previews */}
      <audio ref={audioRef} onEnded={() => setPlayingId(null)} className="hidden">
        <track kind="captions" />
      </audio>

      <div className="text-center space-y-2">
        {demo ? (
          <span className="mb-2 inline-block border border-amber-500/50 px-2 py-0.5 text-[10px] uppercase tracking-wide text-amber-500">
            Demo — sample speakers
          </span>
        ) : null}
        <h1 className="text-lg lg:text-xl mb-4 font-serif">Pick your speaker</h1>
        <p className="font-sans text-sm text-[#878787]">
          We found multiple voices. Choose the one to keep — the dataset will contain only this
          speaker.
        </p>
      </div>

      <div className="mt-8 space-y-2">
        {speakers.map(({ id, sample }) => (
          <div
            key={id}
            role="radio"
            aria-checked={selectedId === id}
            tabIndex={0}
            onClick={() => setSelectedId(id)}
            onKeyDown={(e) => {
              if (e.key === "Enter" || e.key === " ") {
                e.preventDefault();
                setSelectedId(id);
              }
            }}
            className={cn(
              "flex w-full cursor-pointer items-center gap-3 border px-4 py-3 text-left transition-colors",
              selectedId === id ? "border-primary" : "border-border hover:bg-secondary/40",
            )}
          >
            <button
              type="button"
              aria-label={playingId === id ? `Pause ${id}` : `Preview ${id}`}
              onClick={(e) => {
                e.stopPropagation();
                togglePlay(id, sample.sample_id);
              }}
              className="flex h-9 w-9 flex-shrink-0 items-center justify-center rounded-full border border-border bg-secondary"
            >
              {playingId === id ? <Icons.Pause size={16} /> : <Icons.Play size={16} />}
            </button>
            <div className="min-w-0 flex-1">
              <div className="flex items-baseline justify-between">
                <span className="text-sm font-medium">{id.replace(/_/g, " ")}</span>
                <span className="text-xs text-muted-foreground">
                  {sample.duration_sec.toFixed(1)}s sample
                </span>
              </div>
            </div>
          </div>
        ))}
      </div>

      <div className="mt-6 flex items-center justify-between">
        <DiagnosticsSheet runId={runId} runStatus={runStatus} runStage="diarization" />
        <SubmitButton
          type="button"
          disabled={!selectedId || busy}
          onClick={() => void confirm()}
          isSubmitting={busy}
          className="px-4 py-2 bg-primary text-primary-foreground font-medium text-sm hover:bg-primary/90 transition-colors"
        >
          {selectedId ? `Continue with ${selectedId.replace(/_/g, " ")}` : "Select a voice"}
        </SubmitButton>
      </div>
    </>
  );
}
