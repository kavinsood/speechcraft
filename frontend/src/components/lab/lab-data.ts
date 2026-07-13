// Mock data + types + helpers for the Clip Lab workstation.
//
// This is the no-backend mock phase: clips, projects, peaks, QC scores are
// all synthesized client-side so the full Lab UX can be built and felt
// before the Python/FastAPI backend exists. Shapes intentionally mirror
// speechcraft's real Lab data model (review status, QC buckets, revisions,
// EDL edits, provenance) so wiring the real API later is a swap, not a
// redesign.

export type ReviewStatus = "unresolved" | "accepted" | "rejected" | "quarantined";
export type MachineBucket = "auto_kept" | "needs_review" | "auto_rejected";

export type ClipRevision = {
  id: string;
  message: string;
  status: ReviewStatus;
  transcript: string;
  createdAt: string;
  milestone: boolean;
};

export type ClipEdit = {
  op: string;
  startSeconds?: number;
  endSeconds?: number;
  durationSeconds?: number;
};

export type LabClip = {
  id: string;
  order: number;
  transcript: string;
  originalTranscript: string;
  durationSeconds: number;
  status: ReviewStatus;
  machineBucket: MachineBucket;
  qcScore: number; // 0..1 aggregate
  transcriptConfidence: number; // 0..100 (0 when unscored — see transcriptMatchRaw)
  speakerPurity: number; // 0..100 (0 when unscored — see speakerCheckRaw)
  // Raw nullable scores, distinct from the 0-defaulted fields above. null
  // means "unscored" (backend returned null) — used wherever "unscored" must
  // be told apart from "scored 0", e.g. re-deriving machineBucket when
  // thresholds change after the clip was already mapped.
  transcriptMatchRaw: number | null;
  speakerCheckRaw: number | null;
  variant: string; // "source" or a model name
  tags: string[];
  reasonCodes: string[];
  peaks: number[]; // mock waveform envelope, 0..1
  sampleRateHz: number;
  channels: number;
  speaker: string;
  language: string;
  sourceRecording: string;
  originalStartSeconds: number;
  originalEndSeconds: number;
  revisions: ClipRevision[];
  edits: ClipEdit[];
  // Present when backed by the live speechcraft API (null in mock mode).
  audioUrl?: string | null;
  waveformPeaksUrl?: string | null;
  // Backend optimistic-concurrency + audio-op tokens (live-backed only).
  clipVersion?: number;
  manifestSha?: string;
  effectiveAudioRevisionKey?: string | null;
  renderStatus?: "ready" | "pending" | "failed";
  canUndoAudio?: boolean;
  canRedoAudio?: boolean;
  audioEditOpCount?: number;
};

export type LabProject = {
  id: string;
  name: string;
};

export const REVIEW_STATUS_ORDER: ReviewStatus[] = [
  "unresolved",
  "accepted",
  "rejected",
  "quarantined",
];

export const STATUS_LABELS: Record<ReviewStatus, string> = {
  unresolved: "Unresolved",
  accepted: "Accepted",
  rejected: "Rejected",
  quarantined: "Quarantined",
};

export const MACHINE_BUCKET_LABELS: Record<MachineBucket, string> = {
  auto_kept: "Auto-kept",
  needs_review: "Needs review",
  auto_rejected: "Auto-rejected",
};

export const MACHINE_BUCKET_ORDER: MachineBucket[] = [
  "auto_kept",
  "needs_review",
  "auto_rejected",
];

export const REASON_CODE_LABELS: Record<string, string> = {
  broken_audio: "Broken audio",
  near_silence_unusable_clip: "Near-silent / unusable",
  transcript_mismatch: "Transcript mismatch",
  severe_clipping_corruption: "Severe clipping / corruption",
  overlap_second_speaker: "Second speaker overlap",
};

// ── Formatting ──────────────────────────────────────────────────────────

/** "5.37s" style duration. */
export function formatSeconds(seconds: number): string {
  return `${seconds.toFixed(2)}s`;
}

/** "03.49" style clock (seconds.centiseconds), matching the reference UI. */
export function formatClock(seconds: number): string {
  const whole = Math.floor(seconds);
  const centis = Math.round((seconds - whole) * 100);
  return `${whole.toString().padStart(2, "0")}.${centis.toString().padStart(2, "0")}`;
}

/** Compact "15m 59s" style for aggregate durations. */
export function formatDurationCompact(totalSeconds: number): string {
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = Math.round(totalSeconds % 60);
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

/** QC score cards render 0..100 with two decimals (screenshot shows 40.53). */
export function formatQcScore(value: number): string {
  return value.toFixed(2);
}

// ── Deterministic mock peaks ────────────────────────────────────────────

function seededRandom(seed: number): () => number {
  let s = seed % 2147483647;
  if (s <= 0) s += 2147483646;
  return () => {
    s = (s * 16807) % 2147483647;
    return (s - 1) / 2147483646;
  };
}

function hashString(input: string): number {
  let hash = 0;
  for (let i = 0; i < input.length; i++) {
    hash = (hash * 31 + input.charCodeAt(i)) & 0x7fffffff;
  }
  return hash || 1;
}

/** Speech-like envelope: bursts of energy with gaps, deterministic per seed. */
export function generatePeaks(seed: string, count = 240): number[] {
  const rand = seededRandom(hashString(seed));
  const peaks: number[] = [];
  let envelope = 0;
  let burstPhase = rand() * Math.PI * 2;

  for (let i = 0; i < count; i++) {
    burstPhase += 0.18 + rand() * 0.06;
    // Gate creates silence gaps between "words".
    const gate = Math.sin(burstPhase) > -0.35 ? 1 : 0.06;
    const target = gate * (0.25 + rand() * 0.75);
    envelope = envelope * 0.55 + target * 0.45;
    peaks.push(Math.min(1, Math.max(0.015, envelope)));
  }
  return peaks;
}

// ── Mock projects + clips ───────────────────────────────────────────────

export const MOCK_PROJECTS: LabProject[] = [
  { id: "mb-mq3wkz25", name: "mb" },
  { id: "emma-watson-01", name: "Emma Watson" },
  { id: "billie-rae", name: "Hey Billie Rae" },
];

const MOCK_TRANSCRIPTS = [
  "Isn't this must be like stress relieving and Calming what if I just make people",
  "thought it was gonna do and wanted it to do and more but um, I wanted this single especially to be kind of like the",
  "Everything's like a stepping stone in regards to the singles I put out. You know,",
  "have so many lyrical connections Like they all have there's like words that are the same in a lot of them",
  "and hearts like hell home with you All the H's and whatever. I was people have said",
  "I think the biggest thing for me was just learning how to trust the process a little bit more",
  "we spent probably three weeks just on the drum sounds alone which sounds insane now",
  "but when you hear it back on a proper system it makes all the difference honestly",
  "there's a moment right before the second chorus where everything drops out and it's just the vocal",
  "and I remember the engineer looking at me like are you sure you want to do that",
  "yeah so that was recorded in a tiny room in the back of my parents house believe it or not",
  "the reverb you hear is actually the natural room we didn't add anything to it",
  "I wanted it to feel intimate like you're sitting right there in the room with me",
  "and honestly some of the takes that made the record were the scratch vocals",
  "we tried to re-record them cleaner but they just didn't have the same feeling",
  "there's an imperfection to it that I think people connect with more than a perfect take",
  "the bridge went through maybe fifteen different versions before we landed on this one",
  "at one point it was twice as long and had a whole extra section that we cut",
  "sometimes the best thing you can do is take something away rather than add to it",
  "I learned that from working with producers who are way more experienced than me",
];

const SPEAKERS = ["SPEAKER_00", "SPEAKER_01", "SPEAKER_00", "SPEAKER_02"];
const REASON_POOL = [
  "transcript_mismatch",
  "overlap_second_speaker",
  "near_silence_unusable_clip",
];

export function createMockClips(count = 42): LabClip[] {
  const rand = seededRandom(20260712);
  const clips: LabClip[] = [];

  for (let i = 0; i < count; i++) {
    const id = `candidate_review_clip_${i.toString().padStart(6, "0")}`;
    const transcript = MOCK_TRANSCRIPTS[i % MOCK_TRANSCRIPTS.length]!;
    const duration = Number((3 + rand() * 9).toFixed(2));
    const qcScore = Number(rand().toFixed(3));

    // Machine bucket derived from score, roughly mirroring backend thresholds.
    const machineBucket: MachineBucket =
      qcScore >= 0.72 ? "auto_kept" : qcScore < 0.35 ? "auto_rejected" : "needs_review";

    const hasReason = qcScore < 0.4 && rand() > 0.5;
    const reasonCodes = hasReason
      ? [REASON_POOL[Math.floor(rand() * REASON_POOL.length)]!]
      : [];

    const createdAt = new Date(2026, 5, 7, 5, 30, i).toISOString();

    clips.push({
      id,
      order: i + 1,
      transcript,
      originalTranscript: transcript,
      durationSeconds: duration,
      status: "unresolved",
      machineBucket,
      qcScore,
      transcriptConfidence: Number((25 + rand() * 70).toFixed(2)),
      speakerPurity: Number((60 + rand() * 39).toFixed(2)),
      transcriptMatchRaw: Number((25 + rand() * 70).toFixed(2)),
      speakerCheckRaw: Number((60 + rand() * 39).toFixed(2)),
      variant: "source",
      tags: [],
      reasonCodes,
      peaks: generatePeaks(id),
      sampleRateHz: 16000,
      channels: 2,
      speaker: SPEAKERS[i % SPEAKERS.length]!,
      language: "en",
      sourceRecording: `recording_${(i % 3) + 1}`,
      originalStartSeconds: Number((i * 6.4).toFixed(2)),
      originalEndSeconds: Number((i * 6.4 + duration).toFixed(2)),
      revisions: [
        {
          id: `${id}-rev-0`,
          message: "Dataset candidate clip",
          status: "unresolved",
          transcript,
          createdAt,
          milestone: false,
        },
      ],
      edits: [],
    });
  }

  return clips;
}

// ── Sorting + filtering ─────────────────────────────────────────────────

export type SortMode = "source" | "qc_desc" | "qc_asc" | "status";

export const SORT_OPTIONS: { value: SortMode; label: string }[] = [
  { value: "source", label: "Source Order" },
  { value: "qc_desc", label: "QC Score (high → low)" },
  { value: "qc_asc", label: "QC Score (low → high)" },
  { value: "status", label: "Review Status" },
];

const STATUS_PRIORITY: Record<ReviewStatus, number> = {
  unresolved: 0,
  quarantined: 1,
  rejected: 2,
  accepted: 3,
};

export function sortClips(clips: LabClip[], mode: SortMode): LabClip[] {
  const copy = [...clips];
  switch (mode) {
    case "qc_desc":
      return copy.sort((a, b) => b.qcScore - a.qcScore);
    case "qc_asc":
      return copy.sort((a, b) => a.qcScore - b.qcScore);
    case "status":
      return copy.sort(
        (a, b) =>
          STATUS_PRIORITY[a.status] - STATUS_PRIORITY[b.status] || a.order - b.order,
      );
    default:
      return copy.sort((a, b) => a.order - b.order);
  }
}

export function filterClips(
  clips: LabClip[],
  search: string,
  statuses: ReviewStatus[],
  tags: string[],
  buckets: MachineBucket[] = [],
): LabClip[] {
  const query = search.trim().toLowerCase();
  return clips.filter((clip) => {
    if (query && !clip.transcript.toLowerCase().includes(query)) return false;
    if (statuses.length > 0 && !statuses.includes(clip.status)) return false;
    if (tags.length > 0 && !tags.some((tag) => clip.tags.includes(tag))) return false;
    if (buckets.length > 0 && !buckets.includes(clip.machineBucket)) return false;
    return true;
  });
}
