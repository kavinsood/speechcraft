// Write client for speechcraft's live FastAPI backend (same-origin via the
// /sc-api rewrite). Every clip-lab mutation carries optimistic-concurrency
// tokens (expected_manifest_sha256 + expected_clip_version); the backend
// returns 409 if either is stale. Error responses are mapped into a typed
// SpeechcraftApiError so the workstation can react per failure kind.

const BASE = process.env.NEXT_PUBLIC_SPEECHCRAFT_API_URL ?? "/sc-api";

// ── Error taxonomy ──────────────────────────────────────────────────────
// Mirrors the HTTPException status codes raised in backend/app/main.py:
//   404 not found · 409 stale manifest/clip · 422 unrendered audio ·
//   400 validation · 500 render failure · 503 clip-lab state/lock error.
export type ApiErrorKind =
  | "not_found"
  | "conflict"
  | "unrendered"
  | "validation"
  | "render"
  | "unavailable"
  | "unknown";

export class SpeechcraftApiError extends Error {
  readonly status: number;
  readonly kind: ApiErrorKind;
  readonly detail: string;

  constructor(status: number, detail: string) {
    super(detail || `Speechcraft API error ${status}`);
    this.name = "SpeechcraftApiError";
    this.status = status;
    this.detail = detail;
    this.kind = SpeechcraftApiError.kindFor(status);
  }

  static kindFor(status: number): ApiErrorKind {
    switch (status) {
      case 404:
        return "not_found";
      case 409:
        return "conflict";
      case 422:
        return "unrendered";
      case 400:
        return "validation";
      case 500:
        return "render";
      case 503:
        return "unavailable";
      default:
        return "unknown";
    }
  }

  /** True when the client's optimistic tokens are stale and it must refetch. */
  get isStale(): boolean {
    return this.kind === "conflict";
  }
}

async function parseError(res: Response): Promise<SpeechcraftApiError> {
  let detail = `${res.status} ${res.statusText}`;
  try {
    const body = await res.json();
    if (body && typeof body === "object" && "detail" in body) {
      const d = (body as { detail: unknown }).detail;
      detail = typeof d === "string" ? d : JSON.stringify(d);
    }
  } catch {
    // non-JSON error body; keep status line
  }
  return new SpeechcraftApiError(res.status, detail);
}

async function sendJson<T>(
  path: string,
  method: "POST" | "PATCH",
  body: unknown,
): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as T;
}

// ── Payload + response types (mirror backend/app/models.py) ─────────────
export type DatasetAudioEditOperation =
  | { kind: "delete_range"; start_sample: number; end_sample: number }
  | { kind: "insert_silence"; at_sample: number; duration_samples: number };

export type DatasetClipLabPatchRequest = {
  expected_manifest_sha256: string;
  expected_clip_version: number;
  review_status?: "unresolved" | "accepted" | "rejected" | "quarantined";
  transcript_override?: string | null;
  reviewer_tags?: string[];
};

export type DatasetClipLabAudioOperationRequest = {
  expected_manifest_sha256: string;
  expected_clip_version: number;
  operation: DatasetAudioEditOperation;
};

export type DatasetClipLabAudioStackRequest = {
  expected_manifest_sha256: string;
  expected_clip_version: number;
};

// Full clip row returned by every write (backend DatasetClipLabClipView).
export type DatasetClipLabClipView = {
  clip_id: string;
  clip_version: number;
  review_status: "unresolved" | "accepted" | "rejected" | "quarantined";
  transcript: string;
  original_transcript: string;
  transcript_override: string | null;
  reviewer_tags: string[];
  pipeline_findings: { code: string; label: string }[];
  content_hash: string;
  accepted_content_hash: string | null;
  acceptance_stale: boolean;
  transcript_match: number | null;
  speaker_check: number | null;
  sample_rate_hz: number | null;
  effective_audio_kind: "candidate_original" | "rendered_revision" | null;
  effective_audio_revision_key: string | null;
  audio_url: string | null;
  waveform_peaks_url: string | null;
  current_duration_sec: number | null;
  audio_edit_op_count: number;
  audio_edit_ops: DatasetAudioEditOperation[];
  can_undo_audio: boolean;
  can_redo_audio: boolean;
  render_status: "ready" | "pending" | "failed";
};

export type MarkReferenceClipCandidateRequest = {
  clip_id: string;
  transcript_text: string;
};

export type ReferenceClipCandidateView = {
  project_id: string;
  dataset_run_id: string;
  clip_id: string;
  transcript_text: string;
  filename: string;
  relative_path: string;
  source_audio_path: string;
  created_at: string;
};

export type DatasetWaveformPeaksPayload = {
  revision_key: string;
  bins: number;
  peaks: number[];
  duration_sec: number;
  sample_rate_hz: number;
};

// ── Write operations ────────────────────────────────────────────────────
const clipPath = (runId: string, clipId: string) =>
  `/api/dataset-runs/${runId}/clips/${encodeURIComponent(clipId)}`;

/** Review status / transcript override / reviewer tags. */
export function patchClipLab(
  runId: string,
  clipId: string,
  payload: DatasetClipLabPatchRequest,
): Promise<DatasetClipLabClipView> {
  return sendJson(`${clipPath(runId, clipId)}/clip-lab`, "PATCH", payload);
}

/** Append an EDL audio operation (delete range / insert silence). */
export function appendAudioOperation(
  runId: string,
  clipId: string,
  payload: DatasetClipLabAudioOperationRequest,
): Promise<DatasetClipLabClipView> {
  return sendJson(`${clipPath(runId, clipId)}/audio/operations`, "POST", payload);
}

export function undoAudioOperation(
  runId: string,
  clipId: string,
  payload: DatasetClipLabAudioStackRequest,
): Promise<DatasetClipLabClipView> {
  return sendJson(`${clipPath(runId, clipId)}/audio/undo`, "POST", payload);
}

export function redoAudioOperation(
  runId: string,
  clipId: string,
  payload: DatasetClipLabAudioStackRequest,
): Promise<DatasetClipLabClipView> {
  return sendJson(`${clipPath(runId, clipId)}/audio/redo`, "POST", payload);
}

/** Save the active clip as a reference-asset candidate for the project. */
export function markReferenceClipCandidate(
  projectId: string,
  runId: string,
  payload: MarkReferenceClipCandidateRequest,
): Promise<ReferenceClipCandidateView> {
  return sendJson(
    `/api/projects/${projectId}/dataset-runs/${runId}/reference-clip-candidates`,
    "POST",
    payload,
  );
}

/** Fetch real waveform peaks for a clip revision (path from the clip view). */
export async function fetchWaveformPeaks(
  pathOrUrl: string,
): Promise<DatasetWaveformPeaksPayload> {
  const url = pathOrUrl.startsWith("http") ? pathOrUrl : `${BASE}${pathOrUrl}`;
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as DatasetWaveformPeaksPayload;
}

// ── Dataset Health (QC) finalize ────────────────────────────────────────
// POST /api/dataset-runs/{run_id}/qc/finalize — commits the chosen thresholds
// (+ optional manual overrides) as the durable QC verdict. This writes
// dataset_qc.json AND clears prior export artifacts, so it is a deliberate,
// confirmed action, not something to fire on every slider drag. 400 on
// validation (e.g. QC not ready), 404 unknown run.
export type DatasetQcManualOverride = {
  clip_id: string;
  override: "force_keep" | "force_reject";
  reason?: string;
};

export type DatasetQcFinalizeResponse = {
  run_id: string;
  dataset_qc_path: string;
  summary: {
    accepted_count: number;
    rejected_count: number;
    accepted_duration_sec: number;
    rejected_duration_sec: number;
  };
};

export async function finalizeDatasetQc(
  runId: string,
  thresholds: { transcript_match_min: number; speaker_check_min: number },
  manualOverrides: DatasetQcManualOverride[] = [],
): Promise<DatasetQcFinalizeResponse> {
  return sendJson<DatasetQcFinalizeResponse>(
    `/api/dataset-runs/${runId}/qc/finalize`,
    "POST",
    { thresholds, manual_overrides: manualOverrides },
  );
}
