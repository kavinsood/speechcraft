// Client for speechcraft's live FastAPI backend, reached same-origin through
// the /sc-api Next rewrite (see next.config.ts) to avoid CORS and stream
// audio range requests. Read-only for now: projects, dataset runs, and the
// clip-lab view. Writes (status/transcript/edits) still run on local
// optimistic state — wiring those mutations is the next step.

import type { LabClip, MachineBucket, ReviewStatus } from "./lab-data";

const BASE = process.env.NEXT_PUBLIC_SPEECHCRAFT_API_URL ?? "/sc-api";

// Default QC gate thresholds — mirror backend dataset_qc.py
// (DEFAULT_TRANSCRIPT_THRESHOLD / DEFAULT_SPEAKER_THRESHOLD). transcript_match
// and speaker_check are two INDEPENDENT necessary gates (AND); they are never
// blended into one score. These are only the fallback: once Dataset Health
// commits thresholds (POST /qc/finalize), mapApiClip's machineBucket should
// use the committed finalized_thresholds instead — see effectiveQcThresholds.
export const QC_TRANSCRIPT_THRESHOLD = 85;
export const QC_SPEAKER_THRESHOLD = 70;

export type QcThresholds = {
  transcriptMatchMin: number;
  speakerCheckMin: number;
};

export const DEFAULT_QC_THRESHOLDS: QcThresholds = {
  transcriptMatchMin: QC_TRANSCRIPT_THRESHOLD,
  speakerCheckMin: QC_SPEAKER_THRESHOLD,
};

/** Committed thresholds win; otherwise fall back to the backend defaults. */
export function effectiveQcThresholds(payload: DatasetQcPayload | undefined): QcThresholds {
  if (payload?.finalized_thresholds) {
    return {
      transcriptMatchMin: payload.finalized_thresholds.transcript_match_min,
      speakerCheckMin: payload.finalized_thresholds.speaker_check_min,
    };
  }
  if (payload?.defaults) {
    return {
      transcriptMatchMin: payload.defaults.transcript_match_threshold,
      speakerCheckMin: payload.defaults.speaker_check_threshold,
    };
  }
  return DEFAULT_QC_THRESHOLDS;
}

export type SpeechcraftProject = {
  id: string;
  name: string;
};

export type SpeechcraftDatasetRun = {
  id: string;
  project_id: string;
  stage: string;
  status: string;
  output_summary?: { clip_count?: number } | null;
};

export type ClipLabApiClip = {
  clip_id: string;
  clip_version: number;
  content_hash: string;
  transcript_override: string | null;
  effective_audio_revision_key: string | null;
  acceptance_stale: boolean;
  review_status: string;
  transcript: string | null;
  original_transcript: string | null;
  reviewer_tags: string[];
  pipeline_findings: unknown[];
  transcript_match: number | null;
  speaker_check: number | null;
  sample_rate_hz: number | null;
  effective_audio_kind: string | null;
  audio_url: string;
  waveform_peaks_url: string | null;
  current_duration_sec: number | null;
  audio_edit_op_count: number;
  audio_edit_ops: unknown[];
  can_undo_audio: boolean;
  can_redo_audio: boolean;
  render_status: string | null;
};

export type ClipLabApiView = {
  run_id: string;
  candidate_manifest_sha256: string;
  stale_state: boolean;
  qc_available: boolean;
  clips: ClipLabApiClip[];
};

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`, { headers: { Accept: "application/json" } });
  if (!res.ok) {
    throw new Error(`Speechcraft API ${res.status} for ${path}`);
  }
  return (await res.json()) as T;
}

export async function fetchProjects(): Promise<SpeechcraftProject[]> {
  return getJson<SpeechcraftProject[]>("/api/projects");
}

export async function fetchDatasetRuns(projectId: string): Promise<SpeechcraftDatasetRun[]> {
  return getJson<SpeechcraftDatasetRun[]>(`/api/projects/${projectId}/dataset-runs`);
}

export async function fetchClipLabView(runId: string): Promise<ClipLabApiView> {
  return getJson<ClipLabApiView>(`/api/dataset-runs/${runId}/clip-lab`);
}

// ── Dataset Health (QC) ─────────────────────────────────────────────────
// Mirrors backend DatasetQcPayloadView / DatasetQcClipView (models.py).
export type DatasetQcClipApi = {
  clip_id: string;
  audio_path: string;
  audio_url: string;
  duration_sec: number;
  training_text: string;
  alignment_text: string | null;
  transcript_match: number | null; // 0..100, null = unscored (rejected)
  speaker_check: number | null; // 0..100, null = unscored (rejected)
  transcript_reason_codes: string[];
  speaker_reason_codes: string[];
  candidate_reason_codes: string[];
  qc_reason_codes: string[];
  manual_override: "force_keep" | "force_reject" | null;
};

export type DatasetQcPayload = {
  run_id: string;
  ready: boolean;
  missing_artifacts: string[];
  invalid_artifacts: string[];
  defaults: { transcript_match_threshold: number; speaker_check_threshold: number };
  finalized: boolean;
  finalized_thresholds: { transcript_match_min: number; speaker_check_min: number } | null;
  clips: DatasetQcClipApi[];
};

/** Authoritative QC payload for the Dataset Health page (thresholds + per-clip scores). */
export async function fetchDatasetQc(runId: string): Promise<DatasetQcPayload> {
  return getJson<DatasetQcPayload>(`/api/dataset-runs/${runId}/qc`);
}

export type DatasetRunLog = {
  run_id: string;
  path: string;
  text: string;
  truncated: boolean;
};

/** Raw backend worker log for a run (tail-truncated). Diagnostics drawer. */
export async function fetchRunLog(runId: string): Promise<DatasetRunLog> {
  return getJson<DatasetRunLog>(`/api/dataset-runs/${runId}/log`);
}

/** Pick the most relevant run to review: newest run that actually has clips. */
export function pickReviewableRun(
  runs: SpeechcraftDatasetRun[],
): SpeechcraftDatasetRun | null {
  const withClips = runs.find((r) => (r.output_summary?.clip_count ?? 0) > 0);
  return withClips ?? runs[0] ?? null;
}

/** Absolute (same-origin) URL for a clip's audio / peaks endpoint. */
export function resolveMediaUrl(pathOrUrl: string | null | undefined): string | null {
  if (!pathOrUrl) return null;
  if (pathOrUrl.startsWith("http://") || pathOrUrl.startsWith("https://")) return pathOrUrl;
  return `${BASE}${pathOrUrl}`;
}

const KNOWN_STATUSES: ReviewStatus[] = [
  "unresolved",
  "accepted",
  "rejected",
  "quarantined",
];

function normalizeStatus(raw: string): ReviewStatus {
  return (KNOWN_STATUSES as string[]).includes(raw) ? (raw as ReviewStatus) : "unresolved";
}

function findingToCode(finding: unknown): string {
  if (typeof finding === "string") return finding;
  if (finding && typeof finding === "object") {
    const f = finding as Record<string, unknown>;
    return String(f.code ?? f.reason ?? f.type ?? "flag");
  }
  return "flag";
}

/** Map a live clip-lab clip into the view model the panels already consume. */
export function mapApiClip(
  clip: ClipLabApiClip,
  index: number,
  runId: string,
  manifestSha: string,
  thresholds: QcThresholds = DEFAULT_QC_THRESHOLDS,
): LabClip {
  const transcript = clip.transcript ?? clip.original_transcript ?? "";
  const transcriptConfidence = clip.transcript_match ?? 0;
  const speakerPurity = clip.speaker_check ?? 0;

  // Backend-faithful machine bucket: the two scores are independent gates
  // (AND). Hard pipeline findings reject outright; unknown scores can't be
  // machine-decided (needs_review); otherwise pass = both gates cleared.
  // Thresholds are whatever Dataset Health has committed (or the backend
  // defaults if nothing has been committed yet) — never a client-side guess.
  const scoresKnown = clip.transcript_match != null && clip.speaker_check != null;
  const hardFailed = clip.pipeline_findings.length > 0;
  const passesGate =
    scoresKnown &&
    (clip.transcript_match as number) >= thresholds.transcriptMatchMin &&
    (clip.speaker_check as number) >= thresholds.speakerCheckMin;
  const machineBucket: MachineBucket = !scoresKnown
    ? "needs_review"
    : hardFailed || !passesGate
      ? "auto_rejected"
      : "auto_kept";

  // Limiting-gate satisfaction (min across the two gates, each normalized to
  // its own threshold) — NOT a blended score. >=1 means both gates pass; used
  // only to order "closest to failing" in the queue.
  const qcScore = scoresKnown
    ? Math.max(
        0,
        Math.min(
          1,
          Math.min(
            transcriptConfidence / thresholds.transcriptMatchMin,
            speakerPurity / thresholds.speakerCheckMin,
          ),
        ),
      )
    : 0;

  const edits = Array.isArray(clip.audio_edit_ops)
    ? clip.audio_edit_ops.map((op) => {
        const o = (op ?? {}) as Record<string, unknown>;
        return {
          op: String(o.op ?? o.kind ?? "edit"),
          startSeconds: typeof o.start_seconds === "number" ? o.start_seconds : undefined,
          endSeconds: typeof o.end_seconds === "number" ? o.end_seconds : undefined,
          durationSeconds:
            typeof o.duration_seconds === "number" ? o.duration_seconds : undefined,
        };
      })
    : [];

  return {
    id: clip.clip_id,
    order: index + 1,
    transcript,
    originalTranscript: clip.original_transcript ?? transcript,
    durationSeconds: clip.current_duration_sec ?? 0,
    status: normalizeStatus(clip.review_status),
    machineBucket,
    qcScore: Number(qcScore.toFixed(3)),
    transcriptConfidence,
    speakerPurity,
    transcriptMatchRaw: clip.transcript_match,
    speakerCheckRaw: clip.speaker_check,
    variant: clip.effective_audio_kind ?? "source",
    tags: clip.reviewer_tags ?? [],
    reasonCodes: (clip.pipeline_findings ?? []).map(findingToCode),
    peaks: [],
    sampleRateHz: clip.sample_rate_hz ?? 16000,
    channels: 1,
    speaker: "—",
    language: "en",
    sourceRecording: runId,
    originalStartSeconds: 0,
    originalEndSeconds: clip.current_duration_sec ?? 0,
    revisions: [],
    edits,
    audioUrl: resolveMediaUrl(clip.audio_url),
    waveformPeaksUrl: resolveMediaUrl(clip.waveform_peaks_url),
    clipVersion: clip.clip_version,
    manifestSha,
    effectiveAudioRevisionKey: clip.effective_audio_revision_key,
    renderStatus: clip.render_status as LabClip["renderStatus"],
    canUndoAudio: clip.can_undo_audio,
    canRedoAudio: clip.can_redo_audio,
    audioEditOpCount: clip.audio_edit_op_count,
  };
}
