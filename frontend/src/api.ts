import type {
  ExportPreview,
  ExportRun,
  ImportBatch,
  MediaCleanupResult,
  ReferenceAssetDetail,
  ReferenceAssetSummary,
  ReviewStatus,
  Slice,
  SliceSummary,
  SourceRecording,
  WaveformPeaks,
} from "./types";

export const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000";

export class ApiError extends Error {
  readonly status: number;
  readonly url: string;

  constructor(message: string, status: number, url: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.url = url;
  }
}

async function parseJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    let message = `Request failed: ${response.status}`;

    try {
      const payload = (await response.json()) as { detail?: string; message?: string };
      message = payload.detail ?? payload.message ?? message;
    } catch {
      try {
        const text = await response.text();
        if (text.trim()) {
          message = text.trim();
        }
      } catch {
        // Ignore secondary parse failures and keep the status message.
      }
    }

    throw new ApiError(message, response.status, response.url);
  }

  return (await response.json()) as T;
}

export function buildVariantAudioUrl(variantId: string): string {
  return `${API_BASE}/media/variants/${variantId}.wav`;
}

export function buildSliceAudioUrl(sliceId: string, revision?: string): string {
  const url = new URL(`${API_BASE}/media/slices/${sliceId}.wav`);
  if (revision) {
    url.searchParams.set("rev", revision);
  }
  return url.toString();
}

export function buildReferenceVariantAudioUrl(variantId: string): string {
  return `${API_BASE}/media/reference-variants/${variantId}.wav`;
}

export async function fetchHealthStrict(): Promise<{ status: string }> {
  const response = await fetch(`${API_BASE}/healthz`);
  return await parseJson<{ status: string }>(response);
}

export async function fetchProjects(): Promise<ImportBatch[]> {
  const response = await fetch(`${API_BASE}/api/projects`);
  return await parseJson<ImportBatch[]>(response);
}

export async function fetchProject(projectId: string): Promise<ImportBatch> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}`);
  return await parseJson<ImportBatch>(response);
}

export async function fetchProjectSlices(projectId: string): Promise<SliceSummary[]> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/slices`);
  return await parseJson<SliceSummary[]>(response);
}

export async function fetchProjectSourceRecordings(projectId: string): Promise<SourceRecording[]> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/source-recordings`);
  return await parseJson<SourceRecording[]>(response);
}

export async function fetchProjectReferenceAssets(
  projectId: string,
): Promise<ReferenceAssetSummary[]> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/reference-assets`);
  return await parseJson<ReferenceAssetSummary[]>(response);
}

export async function fetchReferenceAsset(assetId: string): Promise<ReferenceAssetDetail> {
  const response = await fetch(`${API_BASE}/api/reference-assets/${assetId}`);
  return await parseJson<ReferenceAssetDetail>(response);
}

export async function fetchSliceDetail(sliceId: string): Promise<Slice> {
  const response = await fetch(`${API_BASE}/api/slices/${sliceId}`);
  return await parseJson<Slice>(response);
}

export async function fetchProjectExports(projectId: string): Promise<ExportRun[]> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/exports`);
  return await parseJson<ExportRun[]>(response);
}

export async function fetchExportPreview(projectId: string): Promise<ExportPreview> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/export-preview`);
  return await parseJson<ExportPreview>(response);
}

export async function runProjectExport(projectId: string): Promise<ExportRun> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/export`, {
    method: "POST",
  });
  return await parseJson<ExportRun>(response);
}

export async function cleanupProjectMedia(projectId: string): Promise<MediaCleanupResult> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/media-cleanup`, {
    method: "POST",
  });
  return await parseJson<MediaCleanupResult>(response);
}

export async function updateClipStatus(clipId: string, status: ReviewStatus): Promise<Slice> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/status`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ status }),
  });
  return await parseJson<Slice>(response);
}

export async function updateClipTranscript(
  clipId: string,
  modifiedText: string,
): Promise<Slice> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/transcript`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ modified_text: modifiedText }),
  });
  return await parseJson<Slice>(response);
}

export async function updateClipTags(
  clipId: string,
  tags: { name: string; color: string }[],
): Promise<Slice> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/tags`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ tags }),
  });
  return await parseJson<Slice>(response);
}

export async function saveClipState(
  clipId: string,
  payload: {
    modified_text?: string | null;
    tags?: { name: string; color: string }[] | null;
    status?: ReviewStatus | null;
    message?: string | null;
    is_milestone?: boolean;
  },
): Promise<Slice> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/save`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return await parseJson<Slice>(response);
}

export async function appendClipEdlOperation(
  clipId: string,
  payload: {
    op: string;
    range?: { start_seconds: number; end_seconds: number } | null;
    duration_seconds?: number | null;
  },
): Promise<Slice> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/edl`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return await parseJson<Slice>(response);
}

export async function undoClip(clipId: string): Promise<Slice> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/undo`, {
    method: "POST",
  });
  return await parseJson<Slice>(response);
}

export async function redoClip(clipId: string): Promise<Slice> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/redo`, {
    method: "POST",
  });
  return await parseJson<Slice>(response);
}

export async function splitClip(clipId: string, splitAtSeconds: number): Promise<SliceSummary[]> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/split`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ split_at_seconds: splitAtSeconds }),
  });
  return await parseJson<SliceSummary[]>(response);
}

export async function mergeWithNextClip(clipId: string): Promise<SliceSummary[]> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/merge-next`, {
    method: "POST",
  });
  return await parseJson<SliceSummary[]>(response);
}

export async function runClipLabModel(
  clipId: string,
  generatorModel: string,
): Promise<Slice> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/variants/run`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ generator_model: generatorModel }),
  });
  return await parseJson<Slice>(response);
}

export async function setActiveVariant(
  clipId: string,
  activeVariantId: string,
): Promise<Slice> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/active-variant`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ active_variant_id: activeVariantId }),
  });
  return await parseJson<Slice>(response);
}

export async function fetchWaveformPeaks(
  clipId: string,
  bins = 120,
): Promise<WaveformPeaks> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/waveform-peaks?bins=${bins}`);
  return await parseJson<WaveformPeaks>(response);
}
