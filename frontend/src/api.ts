import type {
  Clip,
  ClipCommit,
  ClipHistoryResult,
  ClipMutationResult,
  ExportRun,
  ExportPreview,
  Project,
  ProjectDetail,
  ReviewStatus,
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

export function buildClipAudioUrl(clipId: string): string {
  return `${API_BASE}/api/clips/${clipId}/audio`;
}

export async function fetchHealthStrict(): Promise<{ status: string }> {
  const response = await fetch(`${API_BASE}/healthz`);
  return await parseJson<{ status: string }>(response);
}

export async function fetchProjects(): Promise<Project[]> {
  return await fetchProjectsStrict();
}

export async function fetchProjectsStrict(): Promise<Project[]> {
  const response = await fetch(`${API_BASE}/api/projects`);
  return await parseJson<Project[]>(response);
}

export async function fetchProjectDetail(projectId = "phase1-demo"): Promise<ProjectDetail> {
  return await fetchProjectDetailStrict(projectId);
}

export async function fetchProjectDetailStrict(
  projectId = "phase1-demo",
): Promise<ProjectDetail> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}`);
  return await parseJson<ProjectDetail>(response);
}

export async function fetchExportPreview(projectId = "phase1-demo"): Promise<ExportPreview> {
  return await fetchExportPreviewStrict(projectId);
}

export async function fetchExportPreviewStrict(
  projectId = "phase1-demo",
): Promise<ExportPreview> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/export-preview`);
  return await parseJson<ExportPreview>(response);
}

export async function updateClipStatus(
  clipId: string,
  reviewStatus: ReviewStatus,
): Promise<Clip> {
  return await updateClipStatusStrict(clipId, reviewStatus);
}

export async function updateClipStatusStrict(
  clipId: string,
  reviewStatus: ReviewStatus,
): Promise<Clip> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/status`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ review_status: reviewStatus }),
  });
  return await parseJson<Clip>(response);
}

export async function updateClipTranscript(
  clipId: string,
  textCurrent: string,
): Promise<Clip> {
  return await updateClipTranscriptStrict(clipId, textCurrent);
}

export async function updateClipTranscriptStrict(
  clipId: string,
  textCurrent: string,
): Promise<Clip> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/transcript`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ text_current: textCurrent }),
  });
  return await parseJson<Clip>(response);
}

export async function updateClipTags(
  clipId: string,
  tags: { name: string; color: string }[],
): Promise<Clip> {
  return await updateClipTagsStrict(clipId, tags);
}

export async function updateClipTagsStrict(
  clipId: string,
  tags: { name: string; color: string }[],
): Promise<Clip> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/tags`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ tags }),
  });
  return await parseJson<Clip>(response);
}

export async function appendClipEdlOperation(
  clipId: string,
  payload: {
    op: string;
    range?: { start_seconds: number; end_seconds: number } | null;
    duration_seconds?: number | null;
  },
): Promise<Clip> {
  return await appendClipEdlOperationStrict(clipId, payload);
}

export async function appendClipEdlOperationStrict(
  clipId: string,
  payload: {
    op: string;
    range?: { start_seconds: number; end_seconds: number } | null;
    duration_seconds?: number | null;
  },
): Promise<Clip> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/edl`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return await parseJson<Clip>(response);
}

export async function fetchClipCommits(clipId: string): Promise<ClipCommit[]> {
  return await fetchClipCommitsStrict(clipId);
}

export async function fetchClipCommitsStrict(clipId: string): Promise<ClipCommit[]> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/commits`);
  return await parseJson<ClipCommit[]>(response);
}

export async function commitClip(
  clipId: string,
  message = "Manual review commit",
): Promise<ClipCommit> {
  return await commitClipStrict(clipId, message);
}

export async function commitClipStrict(
  clipId: string,
  message = "Manual review commit",
): Promise<ClipCommit> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/commit`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ message }),
  });
  return await parseJson<ClipCommit>(response);
}

export async function undoClip(clipId: string): Promise<ClipHistoryResult> {
  return await undoClipStrict(clipId);
}

export async function undoClipStrict(clipId: string): Promise<ClipHistoryResult> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/undo`, {
    method: "POST",
  });
  return await parseJson<ClipHistoryResult>(response);
}

export async function redoClip(clipId: string): Promise<ClipHistoryResult> {
  return await redoClipStrict(clipId);
}

export async function redoClipStrict(clipId: string): Promise<ClipHistoryResult> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/redo`, {
    method: "POST",
  });
  return await parseJson<ClipHistoryResult>(response);
}

export async function fetchProjectExports(projectId = "phase1-demo"): Promise<ExportRun[]> {
  return await fetchProjectExportsStrict(projectId);
}

export async function fetchProjectExportsStrict(
  projectId = "phase1-demo",
): Promise<ExportRun[]> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/exports`);
  return await parseJson<ExportRun[]>(response);
}

export async function runProjectExport(projectId = "phase1-demo"): Promise<ExportRun> {
  return await runProjectExportStrict(projectId);
}

export async function runProjectExportStrict(
  projectId = "phase1-demo",
): Promise<ExportRun> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/export`, {
    method: "POST",
  });
  return await parseJson<ExportRun>(response);
}

export async function fetchWaveformPeaks(
  clipId: string,
  bins = 120,
): Promise<WaveformPeaks> {
  return await fetchWaveformPeaksStrict(clipId, bins);
}

export async function fetchWaveformPeaksStrict(
  clipId: string,
  bins = 120,
): Promise<WaveformPeaks> {
  const response = await fetch(
    `${API_BASE}/api/clips/${clipId}/waveform-peaks?bins=${bins}`,
  );
  return await parseJson<WaveformPeaks>(response);
}

export async function splitClip(
  clipId: string,
  splitAtSeconds: number,
): Promise<ClipMutationResult> {
  return await splitClipStrict(clipId, splitAtSeconds);
}

export async function splitClipStrict(
  clipId: string,
  splitAtSeconds: number,
): Promise<ClipMutationResult> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/split`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ split_at_seconds: splitAtSeconds }),
  });
  return await parseJson<ClipMutationResult>(response);
}

export async function mergeWithNextClip(clipId: string): Promise<ClipMutationResult> {
  return await mergeWithNextClipStrict(clipId);
}

export async function mergeWithNextClipStrict(
  clipId: string,
): Promise<ClipMutationResult> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/merge-next`, {
    method: "POST",
  });
  return await parseJson<ClipMutationResult>(response);
}
