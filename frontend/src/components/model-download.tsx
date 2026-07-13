"use client";

import { Icons } from "@midday/ui/icons";
import { SubmitButton } from "@midday/ui/submit-button";
import { Spinner } from "@midday/ui/spinner";
import { useRouter, useSearchParams } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";
import { SpeechcraftApiError } from "@/components/lab/speechcraft-write-api";
import { demoEnabled, withDemo } from "@/lib/demo";
import {
  createDatasetRun,
  resolveWhisperModel,
  startRun,
} from "./wizard/wizard-api";

// No knobs: the most capable Whisper model the hardware can load is resolved
// automatically (large-v3, else base). If neither loads, that's a hard error
// with the backend reason surfaced — not a silent spinner.
type Phase = "resolving" | "starting" | "error";

export function ModelDownload() {
  const router = useRouter();
  const params = useSearchParams();
  const projectId = params.get("project");
  const demo = demoEnabled(params);
  const [phase, setPhase] = useState<Phase>("resolving");
  const [model, setModel] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const startedRef = useRef(false);

  const skipAhead = useCallback(() => {
    router.push(withDemo(`/login4?project=${encodeURIComponent(projectId ?? "demo-review")}&run=demo`, true));
  }, [router, projectId]);

  const run = useCallback(async () => {
    if (demo) {
      skipAhead();
      return;
    }
    if (!projectId) {
      setPhase("error");
      setError("No project in the URL. Restart from ingest.");
      return;
    }
    setPhase("resolving");
    setError(null);
    try {
      const resolved = await resolveWhisperModel();
      if (resolved.model === null) {
        setPhase("error");
        setError(resolved.error);
        return;
      }
      setModel(resolved.model);
      setPhase("starting");
      const datasetRun = await createDatasetRun(projectId, {
        whisper_model_size: resolved.model,
      });
      await startRun(datasetRun.id);
      router.push(
        withDemo(
          `/login4?project=${encodeURIComponent(projectId)}&run=${encodeURIComponent(datasetRun.id)}`,
          demo,
        ),
      );
    } catch (err) {
      setPhase("error");
      setError(err instanceof SpeechcraftApiError ? err.detail : "Could not prepare the model.");
    }
  }, [projectId, router, demo, skipAhead]);

  useEffect(() => {
    if (startedRef.current) return;
    startedRef.current = true;
    void run();
  }, [run]);

  if (phase === "error") {
    return (
      <>
        <div className="text-center space-y-2">
          <h1 className="text-lg lg:text-xl mb-4 font-serif">Couldn&apos;t prepare a model</h1>
          <p className="font-sans text-sm text-[#878787]">{error}</p>
        </div>
        <div className="mt-10 lg:mt-12">
          <SubmitButton
            type="button"
            onClick={() => {
              startedRef.current = false;
              void run();
            }}
            className="bg-primary px-6 py-4 text-secondary font-medium flex space-x-2 h-[40px] w-full"
            isSubmitting={false}
          >
            <div className="flex items-center justify-center gap-2">
              <Icons.HuggingFace size={16} />
              <span>Retry</span>
            </div>
          </SubmitButton>
        </div>
        <button
          type="button"
          onClick={skipAhead}
          className="mt-4 w-full text-center text-xs text-[#878787] underline hover:text-foreground"
        >
          Skip ahead with demo data (no model needed)
        </button>
      </>
    );
  }

  return (
    <>
      <div className="text-center space-y-2">
        <h1 className="text-lg lg:text-xl mb-4 font-serif">Preparing models</h1>
        <p className="font-sans text-sm text-[#878787]">
          {phase === "resolving"
            ? "Detecting your hardware and loading the best Whisper model…"
            : `Using ${model} · starting speaker detection…`}
        </p>
      </div>
      <div className="mt-10 lg:mt-12 flex items-center justify-center">
        <Spinner size={20} />
      </div>
    </>
  );
}
