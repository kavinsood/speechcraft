// Wizard-side client for the live FastAPI backend (same-origin via /sc-api).
// Covers project creation + streamed recording upload; run/diarization/
// processing calls are added as their wizard steps are wired.

import { SpeechcraftApiError } from "@/components/lab/speechcraft-write-api";

const BASE = process.env.NEXT_PUBLIC_SPEECHCRAFT_API_URL ?? "/sc-api";

export type SpeechcraftProject = {
  id: string;
  name: string;
};

export type SourceRecording = {
  id: string;
  display_name?: string | null;
  duration_sec?: number | null;
  sample_rate_hz?: number | null;
  channels?: number | null;
};

async function parseError(res: Response): Promise<SpeechcraftApiError> {
  let detail = `${res.status} ${res.statusText}`;
  try {
    const body = await res.json();
    if (body && typeof body === "object" && "detail" in body) {
      const d = (body as { detail: unknown }).detail;
      detail = typeof d === "string" ? d : JSON.stringify(d);
    }
  } catch {
    /* keep status line */
  }
  return new SpeechcraftApiError(res.status, detail);
}

/** Create a project (import batch). id must be url-safe and unique. */
export async function createProject(
  id: string,
  name: string,
): Promise<SpeechcraftProject> {
  const res = await fetch(`${BASE}/api/import-batches`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({ id, name }),
  });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as SpeechcraftProject;
}

/**
 * Stream a single recording into a project, reporting 0..100 upload progress.
 * Uses XHR because fetch has no upload-progress events.
 */
export function uploadRecording(
  projectId: string,
  file: File,
  onProgress: (pct: number) => void,
): Promise<SourceRecording> {
  const url = `${BASE}/api/projects/${projectId}/source-recordings/upload`;
  return new Promise((resolve, reject) => {
    const form = new FormData();
    form.append("file", file, file.name);
    const req = new XMLHttpRequest();
    req.open("POST", url);
    req.upload.onprogress = (e) => {
      if (e.lengthComputable && e.total > 0) {
        onProgress(Math.max(0, Math.min(100, Math.round((e.loaded / e.total) * 100))));
      }
    };
    req.onload = () => {
      let payload: unknown = null;
      try {
        payload = req.responseText ? JSON.parse(req.responseText) : null;
      } catch {
        payload = null;
      }
      if (req.status >= 200 && req.status < 300 && payload) {
        onProgress(100);
        resolve(payload as SourceRecording);
        return;
      }
      const detail =
        payload && typeof payload === "object" && "detail" in payload
          ? String((payload as { detail?: unknown }).detail)
          : `Upload failed (${req.status})`;
      reject(new SpeechcraftApiError(req.status || 0, detail));
    };
    req.onerror = () => reject(new SpeechcraftApiError(0, "Network error during upload"));
    req.send(form);
  });
}

/** Build a url-safe, reasonably-unique project id from a dataset name. */
export function makeProjectId(name: string): string {
  const slug =
    name
      .toLowerCase()
      .replace(/\.[a-z0-9]+$/i, "")
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 24) || "dataset";
  const rand = Math.random().toString(36).slice(2, 8);
  return `${slug}-${rand}`;
}

// ── Run lifecycle ─────────────────────────────────────────────────────────

export type WhisperModelSize = "large-v3" | "base";

export type DatasetRunView = {
  id: string;
  project_id: string;
  stage: string;
  status: string; // pending | running | completed | needs_review | rejected | failed
  input_summary: Record<string, unknown>;
  output_summary: Record<string, unknown>;
  reason_codes: string[];
};

export type PreflightResult = {
  ok: boolean;
  error?: string;
  reason_codes?: string[];
  [k: string]: unknown;
};

export type SpeakerSample = {
  sample_id: string;
  speaker_id: string;
  source_audio_id: string;
  audio_path: string;
  duration_sec: number;
};

export type SpeakerSelection = {
  mode: string;
  selected: boolean;
  target_speaker_id: string | null;
  available_speaker_ids: string[];
};

export type SpeakerResults = {
  run_id: string;
  speaker_regions_summary: { speaker_ids?: string[] } & Record<string, unknown>;
  speaker_samples_manifest: SpeakerSample[];
  speaker_selection: SpeakerSelection | null;
};

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`, { headers: { Accept: "application/json" } });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as T;
}

async function sendJson<T>(path: string, method: "POST" | "PUT", body: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify(body ?? {}),
  });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as T;
}

/** Same-origin media URL for a run artifact / sample audio. */
export function scMediaUrl(pathOrUrl: string | null | undefined): string | null {
  if (!pathOrUrl) return null;
  if (pathOrUrl.startsWith("http://") || pathOrUrl.startsWith("https://")) return pathOrUrl;
  return `${BASE}${pathOrUrl}`;
}

export function speakerSampleUrl(runId: string, sampleId: string): string {
  return `${BASE}/media/dataset-runs/${runId}/speaker-samples/${sampleId}.wav`;
}

/** Capability check — used as the hardware/model resolver. */
export function fetchPreflight(asrModel?: string): Promise<PreflightResult> {
  const q = asrModel ? `?asr_model=${encodeURIComponent(asrModel)}` : "";
  return getJson<PreflightResult>(`/api/system/preflight${q}`);
}

/**
 * Resolve the most capable whisper model the hardware can actually load:
 * prefer large-v3, fall back to base, else the run cannot proceed (error).
 */
export async function resolveWhisperModel(): Promise<
  { model: WhisperModelSize } | { model: null; error: string }
> {
  const large = await fetchPreflight("large-v3");
  if (large.ok) return { model: "large-v3" };
  const base = await fetchPreflight("base");
  if (base.ok) return { model: "base" };
  return {
    model: null,
    error: base.error || large.error || "No usable Whisper model for this hardware.",
  };
}

/** Create a diarization-mode run (single_speaker=false so we can pick the target). */
export function createDatasetRun(
  projectId: string,
  body: {
    source_recording_ids?: string[];
    whisper_model_size: WhisperModelSize;
    language?: string;
    stop_after?: string;
    single_speaker?: boolean;
    config?: Record<string, unknown>;
  },
): Promise<DatasetRunView> {
  return sendJson<DatasetRunView>(`/api/projects/${projectId}/dataset-runs`, "POST", {
    source_recording_ids: body.source_recording_ids ?? [],
    config: body.config ?? {},
    single_speaker: body.single_speaker ?? false, // diarize; output stays single-speaker via selection
    target_speaker_label: "speaker_0",
    language: body.language ?? "auto",
    whisper_model_size: body.whisper_model_size,
    stop_after: body.stop_after ?? "alignment_qc",
  });
}

export function startRun(runId: string): Promise<DatasetRunView> {
  return sendJson<DatasetRunView>(`/api/dataset-runs/${runId}/start`, "POST", {});
}

export function fetchRun(runId: string): Promise<DatasetRunView> {
  return getJson<DatasetRunView>(`/api/dataset-runs/${runId}`);
}

export function refreshRun(runId: string): Promise<DatasetRunView> {
  return sendJson<DatasetRunView>(`/api/dataset-runs/${runId}/refresh`, "POST", {});
}

export function fetchSpeakers(runId: string): Promise<SpeakerResults> {
  return getJson<SpeakerResults>(`/api/dataset-runs/${runId}/speakers`);
}

export function saveSpeakerSelection(
  runId: string,
  targetSpeakerId: string,
): Promise<SpeakerSelection> {
  return sendJson<SpeakerSelection>(`/api/dataset-runs/${runId}/speaker-selection`, "PUT", {
    target_speaker_id: targetSpeakerId,
  });
}

export function resumeProcessing(
  runId: string,
  stopAfter: "buffers" | "normalization" | "mfa" | "alignment_qc" = "alignment_qc",
): Promise<DatasetRunView> {
  return sendJson<DatasetRunView>(`/api/dataset-runs/${runId}/resume-processing`, "POST", {
    stop_after: stopAfter,
  });
}

export function slicerRerun(
  runId: string,
  config: Record<string, number> = {},
): Promise<DatasetRunView> {
  return sendJson<DatasetRunView>(`/api/dataset-runs/${runId}/slicer-rerun`, "POST", { config });
}

export function generateQc(runId: string, force = false): Promise<DatasetRunView> {
  return sendJson<DatasetRunView>(`/api/dataset-runs/${runId}/qc/generate`, "POST", { force });
}

/** Distinct diarized speaker ids from a speakers payload. */
export function distinctSpeakerIds(results: SpeakerResults): string[] {
  const fromSummary = results.speaker_regions_summary?.speaker_ids ?? [];
  const fromSamples = results.speaker_samples_manifest.map((s) => s.speaker_id);
  return Array.from(new Set([...fromSummary, ...fromSamples])).sort();
}

/**
 * Poll a run (via refresh, which advances worker status) until `predicate`
 * holds. Throws on run failure or timeout. Honors an AbortSignal so callers
 * can cancel on unmount.
 */
export async function waitForRun(
  runId: string,
  predicate: (run: DatasetRunView) => boolean,
  opts?: {
    intervalMs?: number;
    timeoutMs?: number;
    signal?: AbortSignal;
    onTick?: (run: DatasetRunView) => void;
  },
): Promise<DatasetRunView> {
  const interval = opts?.intervalMs ?? 2500;
  const timeout = opts?.timeoutMs ?? 3_600_000; // 1h ceiling
  const start = Date.now();
  for (;;) {
    if (opts?.signal?.aborted) throw new SpeechcraftApiError(0, "cancelled");
    const run = await refreshRun(runId);
    opts?.onTick?.(run);
    if (predicate(run)) return run;
    if (run.status === "failed" || run.status === "rejected") {
      throw new SpeechcraftApiError(0, `Run ${run.status} at stage "${run.stage}"`);
    }
    if (Date.now() - start > timeout) {
      throw new SpeechcraftApiError(0, "Timed out waiting for the run");
    }
    await new Promise((r) => setTimeout(r, interval));
  }
}
