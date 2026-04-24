import type {
  ClipLabItem,
  ExportPreview,
  ExportRun,
  ImportBatch,
  MediaCleanupResult,
  ProjectAlignmentSettings,
  PreparationSettings,
  ProjectRecordingJobsRun,
  ProjectPreparationRun,
  ProjectTranscriptionSettings,
  ProcessingJob,
  QcRun,
  QcRunCreateRequest,
  ReferenceAssetDetail,
  ReferenceAssetSummary,
  ReferenceCandidate,
  ReferenceRunRerankResponse,
  ReferenceRun,
  ReviewStatus,
  Slice,
  SliceSummary,
  SlicerRun,
  SlicerRunDeleteResult,
  SlicerRunRequest,
  SourceRecording,
  SourceRecordingQueue,
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

export function resolveApiUrl(pathOrUrl: string): string {
  if (pathOrUrl.startsWith("http://") || pathOrUrl.startsWith("https://")) {
    return pathOrUrl;
  }
  return `${API_BASE}${pathOrUrl}`;
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

export function buildReferenceCandidateAudioUrl(runId: string, candidateId: string): string {
  return `${API_BASE}/media/reference-candidates/${runId}/${candidateId}.wav`;
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

export async function createImportBatch(payload: { id: string; name: string }): Promise<ImportBatch> {
  const response = await fetch(`${API_BASE}/api/import-batches`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return await parseJson<ImportBatch>(response);
}

export async function deleteProject(
  projectId: string,
): Promise<{ project_id: string; deleted_file_count: number }> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}`, {
    method: "DELETE",
  });
  return await parseJson<{ project_id: string; deleted_file_count: number }>(response);
}

export function uploadProjectSourceRecording(
  projectId: string,
  file: File,
  onProgress: (progress: number) => void,
): Promise<SourceRecording> {
  const url = `${API_BASE}/api/projects/${projectId}/source-recordings/upload`;
  return new Promise((resolve, reject) => {
    const request = new XMLHttpRequest();
    request.open("POST", url);
    request.upload.onprogress = (event) => {
      if (!event.lengthComputable || event.total <= 0) {
        return;
      }
      onProgress(Math.max(0, Math.min(100, Math.round((event.loaded / event.total) * 100))));
    };
    request.onload = () => {
      let payload: unknown = null;
      try {
        payload = request.responseText ? JSON.parse(request.responseText) : null;
      } catch {
        payload = null;
      }
      if (request.status >= 200 && request.status < 300 && payload) {
        onProgress(100);
        resolve(payload as SourceRecording);
        return;
      }
      const message =
        payload && typeof payload === "object" && "detail" in payload
          ? String((payload as { detail?: unknown }).detail)
          : `Upload failed: ${request.status}`;
      reject(new ApiError(message, request.status, url));
    };
    request.onerror = () => {
      reject(new ApiError("Upload failed before the server responded.", request.status || 0, url));
    };
    const formData = new FormData();
    formData.append("file", file, file.name);
    request.send(formData);
  });
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

export async function fetchProjectReferenceRuns(projectId: string): Promise<ReferenceRun[]> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/reference-runs`);
  return await parseJson<ReferenceRun[]>(response);
}

export async function createReferenceRun(
  projectId: string,
  payload: {
    recording_ids: string[];
    mode?: "zero_shot" | "finetune" | "both";
    target_durations?: number[] | null;
    candidate_count_cap?: number;
  },
): Promise<ReferenceRun> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/reference-runs`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return await parseJson<ReferenceRun>(response);
}

export async function fetchReferenceRun(runId: string): Promise<ReferenceRun> {
  const response = await fetch(`${API_BASE}/api/reference-runs/${runId}`);
  return await parseJson<ReferenceRun>(response);
}

export async function fetchReferenceRunCandidates(
  runId: string,
  options?: {
    offset?: number;
    limit?: number;
    query?: string;
  },
): Promise<ReferenceCandidate[]> {
  const url = new URL(`${API_BASE}/api/reference-runs/${runId}/candidates`);
  if (options?.offset !== undefined) {
    url.searchParams.set("offset", String(options.offset));
  }
  if (options?.limit !== undefined) {
    url.searchParams.set("limit", String(options.limit));
  }
  if (options?.query) {
    url.searchParams.set("query", options.query);
  }
  const response = await fetch(url.toString());
  return await parseJson<ReferenceCandidate[]>(response);
}

export async function rerankReferenceRunCandidates(
  runId: string,
  payload: {
    positive_candidate_ids: string[];
    negative_candidate_ids: string[];
    mode?: string | null;
  },
): Promise<ReferenceRunRerankResponse> {
  const response = await fetch(`${API_BASE}/api/reference-runs/${runId}/rerank`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return await parseJson<ReferenceRunRerankResponse>(response);
}

export async function fetchReferenceAsset(assetId: string): Promise<ReferenceAssetDetail> {
  const response = await fetch(`${API_BASE}/api/reference-assets/${assetId}`);
  return await parseJson<ReferenceAssetDetail>(response);
}

export async function fetchProjectRecordings(projectId: string): Promise<SourceRecordingQueue[]> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/recordings`);
  return await parseJson<SourceRecordingQueue[]>(response);
}

export async function runProjectTranscription(
  projectId: string,
  settings: ProjectTranscriptionSettings,
): Promise<ProjectRecordingJobsRun> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/transcription`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(settings),
  });
  return await parseJson<ProjectRecordingJobsRun>(response);
}

export async function runProjectAlignment(
  projectId: string,
  settings: ProjectAlignmentSettings,
): Promise<ProjectRecordingJobsRun> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/alignment`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(settings),
  });
  return await parseJson<ProjectRecordingJobsRun>(response);
}

export async function runProjectPreparation(
  projectId: string,
  settings: PreparationSettings,
): Promise<ProjectPreparationRun> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/preparation`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(settings),
  });
  return await parseJson<ProjectPreparationRun>(response);
}

export async function fetchProjectPreparationJobs(projectId: string): Promise<ProcessingJob[]> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/preparation-jobs`);
  return await parseJson<ProcessingJob[]>(response);
}

export async function fetchProcessingJob(jobId: string): Promise<ProcessingJob> {
  const response = await fetch(`${API_BASE}/api/jobs/${jobId}`);
  return await parseJson<ProcessingJob>(response);
}

export async function fetchProjectSlicerRuns(projectId: string): Promise<SlicerRun[]> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/slicer-runs`);
  return await parseJson<SlicerRun[]>(response);
}

export async function createProjectSlicerRun(
  projectId: string,
  payload: SlicerRunRequest,
): Promise<SlicerRun> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/slicer-runs`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return await parseJson<SlicerRun>(response);
}

export async function deleteProjectSlicerRun(
  projectId: string,
  slicerRunId: string,
): Promise<SlicerRunDeleteResult> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/slicer-runs/${slicerRunId}`, {
    method: "DELETE",
  });
  return await parseJson<SlicerRunDeleteResult>(response);
}

export async function fetchProjectQcRuns(projectId: string, slicerRunId?: string): Promise<QcRun[]> {
  const url = new URL(`${API_BASE}/api/projects/${projectId}/qc-runs`);
  if (slicerRunId) {
    url.searchParams.set("slicer_run_id", slicerRunId);
  }
  const response = await fetch(url.toString());
  return await parseJson<QcRun[]>(response);
}

export async function createProjectQcRun(projectId: string, payload: QcRunCreateRequest): Promise<QcRun> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/qc-runs`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return await parseJson<QcRun>(response);
}

export async function fetchQcRun(qcRunId: string): Promise<QcRun> {
  const response = await fetch(`${API_BASE}/api/qc-runs/${qcRunId}`);
  return await parseJson<QcRun>(response);
}

export async function fetchSliceDetail(sliceId: string): Promise<Slice> {
  const response = await fetch(`${API_BASE}/api/slices/${sliceId}`);
  return await parseJson<Slice>(response);
}

export async function fetchClipLabItem(sliceId: string): Promise<ClipLabItem> {
  const response = await fetch(`${API_BASE}/api/slices/${sliceId}/clip-lab`);
  return await parseJson<ClipLabItem>(response);
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

export async function fetchClipLabWaveformPeaks(
  sliceId: string,
  bins = 120,
): Promise<WaveformPeaks> {
  const response = await fetch(`${API_BASE}/api/slices/${sliceId}/waveform-peaks?bins=${bins}`);
  return await parseJson<WaveformPeaks>(response);
}

export async function saveCurrentSliceAsReference(payload: {
  slice_id: string;
  name?: string | null;
  mood_label?: string | null;
}): Promise<ReferenceAssetDetail> {
  const response = await fetch(`${API_BASE}/api/reference-assets/from-slice`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return await parseJson<ReferenceAssetDetail>(response);
}

export async function promoteReferenceCandidate(payload: {
  run_id: string;
  candidate_id: string;
  source_start_seconds?: number | null;
  source_end_seconds?: number | null;
  name?: string | null;
  mood_label?: string | null;
}): Promise<ReferenceAssetDetail> {
  const response = await fetch(`${API_BASE}/api/reference-assets/from-candidate`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return await parseJson<ReferenceAssetDetail>(response);
}
