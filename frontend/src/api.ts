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

const fallbackProject: ProjectDetail = {
  project: {
    id: "phase1-demo",
    name: "Phase 1 Demo Project",
    status: "active",
    export_status: "not_exported",
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  },
  stats: {
    total_clips: 3,
    accepted_clips: 1,
    rejected_clips: 0,
    needs_attention_clips: 1,
    total_duration_seconds: 9.19,
    accepted_duration_seconds: 2.4,
  },
  clips: [
    {
      id: "clip-001",
      project_id: "phase1-demo",
      order_index: 10,
      source_file_id: "src-001",
      working_asset_id: "asset-001",
      original_start_time: 12.4,
      original_end_time: 15.8,
      clip_edl: [
        {
          op: "delete_range",
          range: {
            start_seconds: 0,
            end_seconds: 0.12,
          },
        },
      ],
      review_status: "candidate",
      edit_state: "clean",
      speaker_name: "speaker_a",
      language: "en",
      transcript: {
        text_current: "The workstation should make this painless.",
        text_initial: "The workstation should make this painless.",
        source: "whisper",
        confidence: 0.94,
        updated_at: new Date().toISOString(),
      },
      tags: [{ name: "candidate", color: "#8a7a3d" }],
      is_superseded: false,
      duration_seconds: 3.28,
      sample_rate: 48000,
      channels: 1,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    },
    {
      id: "clip-002",
      project_id: "phase1-demo",
      order_index: 20,
      source_file_id: "src-001",
      working_asset_id: "asset-001",
      original_start_time: 16.1,
      original_end_time: 19.6,
      clip_edl: [],
      review_status: "needs_attention",
      edit_state: "dirty",
      speaker_name: "speaker_a",
      language: "en",
      transcript: {
        text_current: "We can recheck the end consonant here.",
        text_initial: "We can recheck the end consonant here.",
        source: "whisper",
        confidence: 0.71,
        updated_at: new Date().toISOString(),
      },
      tags: [
        { name: "clipped_end", color: "#c95f44" },
        { name: "recheck", color: "#2f6c8f" },
      ],
      is_superseded: false,
      duration_seconds: 3.51,
      sample_rate: 48000,
      channels: 1,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    },
    {
      id: "clip-003",
      project_id: "phase1-demo",
      order_index: 30,
      source_file_id: "src-002",
      working_asset_id: "asset-004",
      original_start_time: 2,
      original_end_time: 4.4,
      clip_edl: [],
      review_status: "accepted",
      edit_state: "committed",
      speaker_name: "speaker_a",
      language: "en",
      transcript: {
        text_current: "This one is already ready for export.",
        text_initial: "This one is already ready for export.",
        source: "manual",
        confidence: 0.98,
        updated_at: new Date().toISOString(),
      },
      tags: [{ name: "clean", color: "#3c8452" }],
      is_superseded: false,
      duration_seconds: 2.4,
      sample_rate: 48000,
      channels: 1,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    },
  ],
};

async function parseJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }

  return (await response.json()) as T;
}

function buildFallbackExportPreview(clips: Clip[], projectId = "phase1-demo"): ExportPreview {
  const accepted = clips.filter((clip) => clip.review_status === "accepted");

  return {
    project_id: projectId,
    manifest_path: `exports/${projectId}/dataset.list`,
    accepted_clip_count: accepted.length,
    lines: accepted.map(
      (clip) =>
        `exports/${projectId}/rendered/${clip.id}.wav|${clip.speaker_name}|${clip.language}|${clip.transcript.text_current}`,
    ),
  };
}

function buildFallbackWaveformPeaks(clip: Clip, bins = 120): WaveformPeaks {
  const seed = clip.id.split("").reduce((sum, char) => sum + char.charCodeAt(0), 0);
  const safeBins = Math.max(16, Math.min(bins, 512));

  return {
    clip_id: clip.id,
    bins: safeBins,
    peaks: Array.from({ length: safeBins }, (_, index) => {
      const raw = Math.sin((seed + index) * 0.57) * 0.5 + 0.5;
      return Number((0.08 + raw * 0.84).toFixed(4));
    }),
  };
}

export async function fetchProjectDetail(projectId = "phase1-demo"): Promise<ProjectDetail> {
  try {
    return await fetchProjectDetailStrict(projectId);
  } catch {
    return fallbackProject;
  }
}

export async function fetchExportPreview(
  projectId = "phase1-demo",
  fallbackClips: Clip[] = fallbackProject.clips,
): Promise<ExportPreview> {
  try {
    return await fetchExportPreviewStrict(projectId);
  } catch {
    return buildFallbackExportPreview(fallbackClips, projectId);
  }
}

export async function updateClipStatus(
  clipId: string,
  reviewStatus: ReviewStatus,
): Promise<Clip | null> {
  try {
    return await updateClipStatusStrict(clipId, reviewStatus);
  } catch {
    return null;
  }
}

export async function updateClipTranscript(
  clipId: string,
  textCurrent: string,
): Promise<Clip | null> {
  try {
    return await updateClipTranscriptStrict(clipId, textCurrent);
  } catch {
    return null;
  }
}

export async function appendClipEdlOperation(
  clipId: string,
  payload: {
    op: string;
    range?: { start_seconds: number; end_seconds: number } | null;
    duration_seconds?: number | null;
  },
): Promise<Clip | null> {
  try {
    return await appendClipEdlOperationStrict(clipId, payload);
  } catch {
    return null;
  }
}

export async function fetchClipCommits(clipId: string): Promise<ClipCommit[]> {
  try {
    return await fetchClipCommitsStrict(clipId);
  } catch {
    return [];
  }
}

export async function commitClip(
  clipId: string,
  message = "Manual review commit",
): Promise<ClipCommit | null> {
  try {
    return await commitClipStrict(clipId, message);
  } catch {
    return null;
  }
}

export async function updateClipTags(
  clipId: string,
  tags: { name: string; color: string }[],
): Promise<Clip | null> {
  try {
    return await updateClipTagsStrict(clipId, tags);
  } catch {
    return null;
  }
}

export async function undoClip(clipId: string): Promise<ClipHistoryResult | null> {
  try {
    return await undoClipStrict(clipId);
  } catch {
    return null;
  }
}

export async function redoClip(clipId: string): Promise<ClipHistoryResult | null> {
  try {
    return await redoClipStrict(clipId);
  } catch {
    return null;
  }
}

export async function fetchProjects(): Promise<Project[]> {
  try {
    return await fetchProjectsStrict();
  } catch {
    return [fallbackProject.project];
  }
}

export async function fetchProjectExports(projectId = "phase1-demo"): Promise<ExportRun[]> {
  try {
    return await fetchProjectExportsStrict(projectId);
  } catch {
    return [];
  }
}

export async function runProjectExport(projectId = "phase1-demo"): Promise<ExportRun | null> {
  try {
    return await runProjectExportStrict(projectId);
  } catch {
    return null;
  }
}

export async function fetchWaveformPeaks(
  clipId: string,
  bins = 120,
): Promise<WaveformPeaks> {
  try {
    return await fetchWaveformPeaksStrict(clipId, bins);
  } catch {
    const clip = fallbackProject.clips.find((candidate) => candidate.id === clipId) ?? fallbackProject.clips[0];
    return buildFallbackWaveformPeaks(clip, bins);
  }
}

export function buildClipAudioUrl(clipId: string): string {
  return `${API_BASE}/api/clips/${clipId}/audio`;
}

export async function fetchHealthStrict(): Promise<{ status: string }> {
  const response = await fetch(`${API_BASE}/healthz`);
  return await parseJson<{ status: string }>(response);
}

export async function fetchProjectsStrict(): Promise<Project[]> {
  const response = await fetch(`${API_BASE}/api/projects`);
  return await parseJson<Project[]>(response);
}

export async function fetchProjectDetailStrict(
  projectId = "phase1-demo",
): Promise<ProjectDetail> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}`);
  return await parseJson<ProjectDetail>(response);
}

export async function fetchExportPreviewStrict(
  projectId = "phase1-demo",
): Promise<ExportPreview> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/export-preview`);
  return await parseJson<ExportPreview>(response);
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

export async function fetchClipCommitsStrict(clipId: string): Promise<ClipCommit[]> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/commits`);
  return await parseJson<ClipCommit[]>(response);
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

export async function undoClipStrict(clipId: string): Promise<ClipHistoryResult> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/undo`, {
    method: "POST",
  });
  return await parseJson<ClipHistoryResult>(response);
}

export async function redoClipStrict(clipId: string): Promise<ClipHistoryResult> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/redo`, {
    method: "POST",
  });
  return await parseJson<ClipHistoryResult>(response);
}

export async function fetchProjectExportsStrict(
  projectId = "phase1-demo",
): Promise<ExportRun[]> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/exports`);
  return await parseJson<ExportRun[]>(response);
}

export async function runProjectExportStrict(
  projectId = "phase1-demo",
): Promise<ExportRun> {
  const response = await fetch(`${API_BASE}/api/projects/${projectId}/export`, {
    method: "POST",
  });
  return await parseJson<ExportRun>(response);
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
): Promise<ClipMutationResult | null> {
  try {
    return await splitClipStrict(clipId, splitAtSeconds);
  } catch {
    return null;
  }
}

export async function mergeWithNextClip(
  clipId: string,
): Promise<ClipMutationResult | null> {
  try {
    return await mergeWithNextClipStrict(clipId);
  } catch {
    return null;
  }
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

export async function mergeWithNextClipStrict(
  clipId: string,
): Promise<ClipMutationResult> {
  const response = await fetch(`${API_BASE}/api/clips/${clipId}/merge-next`, {
    method: "POST",
  });
  return await parseJson<ClipMutationResult>(response);
}
