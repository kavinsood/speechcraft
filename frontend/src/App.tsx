import {
  startTransition,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type WaveSurfer from "wavesurfer.js";
import {
  appendClipEdlOperation,
  buildClipAudioUrl,
  commitClip,
  fetchClipCommits,
  fetchProjects,
  fetchProjectDetail,
  fetchProjectExports,
  mergeWithNextClip,
  redoClip,
  runProjectExport,
  splitClip,
  undoClip,
  updateClipStatus,
  updateClipTags,
  updateClipTranscript,
} from "./api";
import BackendTestPage from "./BackendTestPage";
import WaveformPane from "./WaveformPane";
import type {
  Clip,
  ClipCommit,
  ClipHistoryResult,
  ExportRun,
  ProjectDetail,
  ReviewStatus,
} from "./types";

const queuePriorityOrder: ReviewStatus[] = [
  "candidate",
  "needs_attention",
  "in_review",
  "accepted",
  "rejected",
];

const statusLabels: Record<ReviewStatus, string> = {
  candidate: "Candidate",
  needs_attention: "Needs Attention",
  in_review: "In Review",
  accepted: "Accepted",
  rejected: "Rejected",
};

const playbackRates = [0.5, 0.75, 1, 1.25, 1.5, 2] as const;

type HistoryFlags = {
  can_undo: boolean;
  can_redo: boolean;
};

function formatSeconds(value: number): string {
  return `${value.toFixed(2)}s`;
}

function formatClipTimestamp(value: number): string {
  const totalCentiseconds = Math.max(0, Math.round(value * 100));
  const seconds = Math.floor(totalCentiseconds / 100);
  const centiseconds = totalCentiseconds % 100;
  return `${seconds.toString().padStart(2, "0")}.${centiseconds
    .toString()
    .padStart(2, "0")}`;
}

function formatDurationCompact(totalSeconds: number): string {
  const rounded = Math.max(0, Math.floor(totalSeconds));
  const hours = Math.floor(rounded / 3600);
  const minutes = Math.floor((rounded % 3600) / 60);
  const seconds = rounded % 60;

  if (hours > 0) {
    return `${hours}h ${minutes}m ${seconds}s`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  }
  return `${seconds}s`;
}

function recalculateProjectDetail(detail: ProjectDetail): ProjectDetail {
  const accepted = detail.clips.filter((clip) => clip.review_status === "accepted");
  const rejected = detail.clips.filter((clip) => clip.review_status === "rejected");
  const needsAttention = detail.clips.filter(
    (clip) => clip.review_status === "needs_attention",
  );

  return {
    ...detail,
    stats: {
      ...detail.stats,
      total_clips: detail.clips.length,
      accepted_clips: accepted.length,
      rejected_clips: rejected.length,
      needs_attention_clips: needsAttention.length,
      total_duration_seconds: Number(
        detail.clips.reduce((sum, clip) => sum + clip.duration_seconds, 0).toFixed(2),
      ),
      accepted_duration_seconds: Number(
        accepted.reduce((sum, clip) => sum + clip.duration_seconds, 0).toFixed(2),
      ),
    },
  };
}

function sortClipsForQueue(clips: Clip[]): Clip[] {
  return [...clips].sort((left, right) => {
    const leftPriority = queuePriorityOrder.indexOf(left.review_status);
    const rightPriority = queuePriorityOrder.indexOf(right.review_status);

    if (leftPriority !== rightPriority) {
      return leftPriority - rightPriority;
    }

    if (left.order_index !== right.order_index) {
      return left.order_index - right.order_index;
    }

    return left.created_at.localeCompare(right.created_at);
  });
}

function buildTagColor(name: string): string {
  const palette = [
    "#8a7a3d",
    "#2f6c8f",
    "#c95f44",
    "#3c8452",
    "#8b5fbf",
    "#9a6a2f",
  ];
  const seed = name.split("").reduce((sum, char) => sum + char.charCodeAt(0), 0);
  return palette[seed % palette.length];
}

function parseTagDraft(value: string): { name: string; color: string }[] {
  const seen = new Set<string>();

  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .filter((entry) => {
      const normalized = entry.toLowerCase();
      if (seen.has(normalized)) {
        return false;
      }
      seen.add(normalized);
      return true;
    })
    .map((name) => ({
      name,
      color: buildTagColor(name),
    }));
}

function replaceClipInProject(detail: ProjectDetail, updatedClip: Clip): ProjectDetail {
  return recalculateProjectDetail({
    ...detail,
    clips: detail.clips.map((clip) => (clip.id === updatedClip.id ? updatedClip : clip)),
  });
}

function clipMatchesFilters(
  clip: Clip,
  query: string,
  selectedFilterTags: string[],
  hideResolved: boolean,
): boolean {
  if (hideResolved && (clip.review_status === "accepted" || clip.review_status === "rejected")) {
    return false;
  }

  if (
    selectedFilterTags.length > 0 &&
    !selectedFilterTags.some(
      (selectedTag) => clip.tags.some((tag) => tag.name.toLowerCase() === selectedTag),
    )
  ) {
    return false;
  }

  if (!query) {
    return true;
  }

  const haystacks = [
    clip.id,
    clip.transcript.text_current,
    clip.review_status,
    clip.speaker_name,
    clip.language,
    ...clip.tags.map((tag) => tag.name),
  ];

  return haystacks.some((value) => value.toLowerCase().includes(query));
}

export default function App() {
  if (window.location.pathname === "/backend-test") {
    return <BackendTestPage />;
  }

  const [projectDetail, setProjectDetail] = useState<ProjectDetail | null>(null);
  const [activeClipId, setActiveClipId] = useState<string | null>(null);
  const [draftTranscript, setDraftTranscript] = useState("");
  const [draftTags, setDraftTags] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedFilterTags, setSelectedFilterTags] = useState<string[]>([]);
  const [isTagFilterMenuOpen, setIsTagFilterMenuOpen] = useState(false);
  const [tagInputDraft, setTagInputDraft] = useState("");
  const [hideResolved, setHideResolved] = useState(false);
  const [selectionStart, setSelectionStart] = useState(0);
  const [selectionEnd, setSelectionEnd] = useState(0);
  const [playheadSeconds, setPlayheadSeconds] = useState(0);
  const [hoverSeconds, setHoverSeconds] = useState<number | null>(null);
  const [playbackStartSeconds, setPlaybackStartSeconds] = useState(0);
  const [playbackRate, setPlaybackRate] = useState<(typeof playbackRates)[number]>(1);
  const [isPlaying, setIsPlaying] = useState(false);
  const [exportRuns, setExportRuns] = useState<ExportRun[]>([]);
  const [isRunningExport, setIsRunningExport] = useState(false);
  const [clipCommits, setClipCommits] = useState<Record<string, ClipCommit[]>>({});
  const [historyByClip, setHistoryByClip] = useState<Record<string, HistoryFlags>>({});
  const [isCommittingClip, setIsCommittingClip] = useState(false);
  const [isApplyingEdit, setIsApplyingEdit] = useState(false);
  const [editorNotice, setEditorNotice] = useState<string | null>(null);
  const deferredSearch = useDeferredValue(searchQuery.trim().toLowerCase());
  const waveSurferRef = useRef<WaveSurfer | null>(null);
  const transcriptEditorRef = useRef<HTMLTextAreaElement | null>(null);
  const tagFilterRef = useRef<HTMLDivElement | null>(null);
  const shouldAutoPlayAfterClipChangeRef = useRef(false);

  useEffect(() => {
    let cancelled = false;

    async function loadWorkspace() {
      const projects = await fetchProjects();
      const urlProjectId = new URLSearchParams(window.location.search)
        .get("project")
        ?.trim();
      const sortedProjects = [...projects].sort(
        (left, right) =>
          new Date(right.updated_at).getTime() - new Date(left.updated_at).getTime(),
      );
      const fallbackProjectId = sortedProjects[0]?.id ?? "phase1-demo";
      const selectedProjectId =
        urlProjectId && projects.some((project) => project.id === urlProjectId)
          ? urlProjectId
          : fallbackProjectId;

      const [detail, exports] = await Promise.all([
        fetchProjectDetail(selectedProjectId),
        fetchProjectExports(selectedProjectId),
      ]);

      if (cancelled) {
        return;
      }

      setProjectDetail(detail);
      setExportRuns(exports);
      setActiveClipId(detail.clips[0]?.id ?? null);
      setEditorNotice(null);
    }

    void loadWorkspace();

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!isTagFilterMenuOpen) {
      return;
    }

    const handlePointerDown = (event: MouseEvent) => {
      if (!tagFilterRef.current) {
        return;
      }
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }
      if (!tagFilterRef.current.contains(target)) {
        setIsTagFilterMenuOpen(false);
      }
    };

    document.addEventListener("mousedown", handlePointerDown);
    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
    };
  }, [isTagFilterMenuOpen]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (
        target &&
        (target.tagName === "INPUT" ||
          target.tagName === "TEXTAREA" ||
          target.isContentEditable)
      ) {
        return;
      }

      if (event.ctrlKey || event.metaKey || event.altKey || event.repeat) {
        return;
      }

      if (event.code === "Space") {
        event.preventDefault();
        if (event.shiftKey) {
          void handlePlayFromBeginning();
          return;
        }
        void handleTogglePlayback();
        return;
      }

      if (event.code === "Enter") {
        event.preventDefault();
        if (event.shiftKey) {
          void handleRejectNextAndPlay();
          return;
        }
        void handleAcceptCommitNextAndPlay();
        return;
      }

    };

    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  });

  const queueClips = useMemo(() => {
    const clips = projectDetail?.clips ?? [];
    return sortClipsForQueue(clips).filter((clip) =>
      clipMatchesFilters(clip, deferredSearch, selectedFilterTags, hideResolved),
    );
  }, [projectDetail?.clips, deferredSearch, selectedFilterTags, hideResolved]);

  const availableFilterTags = useMemo(() => {
    const statusTagNames = new Set(queuePriorityOrder.map((status) => status.toLowerCase()));
    const allClipTags = (projectDetail?.clips ?? []).flatMap((clip) =>
      clip.tags
        .map((tag) => tag.name.toLowerCase())
        .filter((tagName) => !statusTagNames.has(tagName)),
    );
    return Array.from(new Set(allClipTags)).sort();
  }, [projectDetail?.clips]);
  const draftTagEntries = useMemo(() => parseTagDraft(draftTags), [draftTags]);
  const suggestedTagNames = useMemo(() => {
    const selected = new Set(draftTagEntries.map((tag) => tag.name.toLowerCase()));
    return availableFilterTags
      .filter((tagName) => !selected.has(tagName.toLowerCase()))
      .slice(0, 12);
  }, [availableFilterTags, draftTagEntries]);

  const activeClip = useMemo(() => {
    const allClips = projectDetail?.clips ?? [];

    return (
      allClips.find((clip) => clip.id === activeClipId) ??
      queueClips[0] ??
      allClips[0] ??
      null
    );
  }, [projectDetail?.clips, activeClipId, queueClips]);

  useEffect(() => {
    if (!activeClip) {
      return;
    }

    setDraftTranscript(activeClip.transcript.text_current);
    setDraftTags(activeClip.tags.map((tag) => tag.name).join(", "));
    setTagInputDraft("");
    setSelectionStart(0);
    setSelectionEnd(0);
    setPlayheadSeconds(0);
    setHoverSeconds(null);
    setPlaybackStartSeconds(0);
    setIsPlaying(false);

    if (shouldAutoPlayAfterClipChangeRef.current) {
      shouldAutoPlayAfterClipChangeRef.current = false;
      window.setTimeout(() => {
        void handleTogglePlayback();
      }, 180);
    }
  }, [activeClip?.id]);

  useEffect(() => {
    const editor = transcriptEditorRef.current;
    if (!editor) {
      return;
    }
    editor.style.height = "auto";
    editor.style.height = `${editor.scrollHeight}px`;
  }, [draftTranscript, activeClipId]);

  useEffect(() => {
    if (!activeClip) {
      return;
    }

    if (clipCommits[activeClip.id]) {
      return;
    }

    let cancelled = false;

    void fetchClipCommits(activeClip.id).then((commits) => {
      if (cancelled) {
        return;
      }

      setClipCommits((current) => ({
        ...current,
        [activeClip.id]: commits,
      }));
    });

    return () => {
      cancelled = true;
    };
  }, [activeClip?.id, clipCommits]);

  function updateCurrentClip(updatedClip: Clip) {
    if (!projectDetail) {
      return;
    }

    setProjectDetail((current) => {
      if (!current) {
        return current;
      }

      return replaceClipInProject(current, updatedClip);
    });
    setHistoryByClip((current) => ({
      ...current,
      [updatedClip.id]: { can_undo: true, can_redo: false },
    }));
  }

  function applyHistoryResult(result: ClipHistoryResult) {
    if (!projectDetail) {
      return;
    }

    setProjectDetail((current) => {
      if (!current) {
        return current;
      }

      return replaceClipInProject(current, result.clip);
    });
    setDraftTranscript(result.clip.transcript.text_current);
    setDraftTags(result.clip.tags.map((tag) => tag.name).join(", "));
    setSelectionStart(0);
    setSelectionEnd(0);
    setPlayheadSeconds(0);
    setHistoryByClip((current) => ({
      ...current,
      [result.clip.id]: {
        can_undo: result.can_undo,
        can_redo: result.can_redo,
      },
    }));
  }

  function handleClipSelect(nextClipId: string) {
    startTransition(() => {
      setActiveClipId(nextClipId);
      setEditorNotice(null);
    });
  }

  function getNextClipId(currentClipId: string): string | null {
    if (queueClips.length === 0) {
      return null;
    }

    const currentIndex = queueClips.findIndex((clip) => clip.id === currentClipId);
    if (currentIndex < 0) {
      return queueClips[0]?.id ?? null;
    }

    const nextClip = queueClips[currentIndex + 1] ?? queueClips[0] ?? null;
    if (!nextClip || nextClip.id === currentClipId) {
      return null;
    }
    return nextClip.id;
  }

  function toggleFilterTag(tagName: string) {
    setSelectedFilterTags((current) =>
      current.includes(tagName)
        ? current.filter((entry) => entry !== tagName)
        : [...current, tagName],
    );
  }

  function addTagToDraft(tagName: string) {
    const normalized = tagName.trim();
    if (!normalized) {
      return;
    }
    const currentTags = parseTagDraft(draftTags).map((tag) => tag.name);
    if (currentTags.some((tag) => tag.toLowerCase() === normalized.toLowerCase())) {
      return;
    }
    const nextTags = [...currentTags, normalized];
    setDraftTags(nextTags.join(", "));
  }

  function removeTagFromDraft(tagName: string) {
    const nextTags = parseTagDraft(draftTags)
      .map((tag) => tag.name)
      .filter((name) => name.toLowerCase() !== tagName.toLowerCase());
    setDraftTags(nextTags.join(", "));
  }

  function handleAddTagFromInput() {
    const value = tagInputDraft.trim();
    if (!value) {
      return;
    }
    addTagToDraft(value);
    setTagInputDraft("");
  }

  function pausePlayback() {
    waveSurferRef.current?.pause();
    setIsPlaying(false);
  }

  function applyPlaybackRate(instance: WaveSurfer | null, rate: number) {
    if (!instance) {
      return;
    }
    // Match browser-native player feel by preserving pitch across rate changes.
    (instance as unknown as { setPlaybackRate: (value: number, preservePitch?: boolean) => void })
      .setPlaybackRate(rate, true);
  }

  function handleCyclePlaybackRate() {
    const currentIndex = playbackRates.indexOf(playbackRate);
    const nextRate = playbackRates[(currentIndex + 1) % playbackRates.length];
    setPlaybackRate(nextRate);
    applyPlaybackRate(waveSurferRef.current, nextRate);
  }

  function setWaveSurferTime(nextTime: number) {
    const waveSurfer = waveSurferRef.current;
    if (!waveSurfer || !activeClip || activeClip.duration_seconds <= 0) {
      return;
    }
    const clamped = Math.max(0, Math.min(nextTime, activeClip.duration_seconds));
    const progress = clamped / activeClip.duration_seconds;
    waveSurfer.seekTo(progress);
    setPlayheadSeconds(Number(clamped.toFixed(2)));
  }

  async function handleStatusChange(reviewStatus: ReviewStatus) {
    if (!activeClip) {
      return;
    }

    if (reviewStatus === "accepted") {
      await handleCommitClip(true);
      return;
    }

    pausePlayback();
    const updatedClip = await updateClipStatus(activeClip.id, reviewStatus);
    if (!updatedClip) {
      setEditorNotice("Status update failed. Check the backend.");
      return;
    }

    updateCurrentClip(updatedClip);
    setEditorNotice(`Marked clip as ${statusLabels[reviewStatus].toLowerCase()}.`);
  }

  async function handleTagsSave() {
    if (!activeClip) {
      return;
    }

    pausePlayback();
    const updatedClip = await updateClipTags(activeClip.id, parseTagDraft(draftTags));
    if (!updatedClip) {
      setEditorNotice("Tag update failed. Check the backend.");
      return;
    }

    updateCurrentClip(updatedClip);
    setDraftTags(updatedClip.tags.map((tag) => tag.name).join(", "));
    setEditorNotice("Saved clip tags.");
  }

  async function handleRunExport() {
    if (!projectDetail) {
      return;
    }

    setIsRunningExport(true);
    const result = await runProjectExport(projectDetail.project.id);
    const [detail, exports] = await Promise.all([
      fetchProjectDetail(projectDetail.project.id),
      fetchProjectExports(projectDetail.project.id),
    ]);
    setProjectDetail(detail);
    setExportRuns(exports);
    setIsRunningExport(false);

    if (!result) {
      setEditorNotice("Export failed. See backend logs for details.");
      return;
    }

    setEditorNotice(`Export completed: ${result.accepted_clip_count} accepted clip(s) rendered.`);
  }

  async function handleDeleteSelection() {
    if (!activeClip) {
      return;
    }

    const duration = activeClip.duration_seconds;
    const start = Math.max(0, Math.min(Math.min(selectionStart, selectionEnd), duration));
    let end = Math.max(0, Math.min(Math.max(selectionStart, selectionEnd), duration));
    const tailSnapThresholdSeconds = 0.03;
    if (duration - end <= tailSnapThresholdSeconds) {
      end = duration;
    }

    if (end <= start) {
      setEditorNotice("Select a non-zero region before deleting.");
      return;
    }

    setIsApplyingEdit(true);
    pausePlayback();
    const updatedClip = await appendClipEdlOperation(activeClip.id, {
      op: "delete_range",
      range: { start_seconds: start, end_seconds: end },
    });
    setIsApplyingEdit(false);

    if (!updatedClip) {
      setEditorNotice("Delete failed. Check the backend.");
      return;
    }

    updateCurrentClip(updatedClip);
    setSelectionStart(Math.min(start, updatedClip.duration_seconds));
    setSelectionEnd(Math.min(start, updatedClip.duration_seconds));
    setPlayheadSeconds(Math.min(start, updatedClip.duration_seconds));
    setWaveSurferTime(Math.min(start, updatedClip.duration_seconds));
    setEditorNotice("Deleted the selected region.");
  }

  async function handleInsertSilence() {
    if (!activeClip) {
      return;
    }

    const cursorTime = waveSurferRef.current
      ? Number(waveSurferRef.current.getCurrentTime().toFixed(2))
      : playheadSeconds;
    const insertAt = Math.max(0, Math.min(cursorTime, activeClip.duration_seconds));
    const silenceDuration = 0.2;

    setIsApplyingEdit(true);
    pausePlayback();
    const updatedClip = await appendClipEdlOperation(activeClip.id, {
      op: "insert_silence",
      range: { start_seconds: insertAt, end_seconds: insertAt },
      duration_seconds: silenceDuration,
    });
    setIsApplyingEdit(false);

    if (!updatedClip) {
      setEditorNotice("Insert silence failed. Check the backend.");
      return;
    }

    updateCurrentClip(updatedClip);
    setSelectionStart(Math.min(insertAt, updatedClip.duration_seconds));
    setSelectionEnd(Math.min(insertAt, updatedClip.duration_seconds));
    setPlayheadSeconds(Math.min(insertAt, updatedClip.duration_seconds));
    setEditorNotice(`Inserted ${formatSeconds(silenceDuration)} of silence.`);
  }

  async function handleUndo() {
    if (!activeClip) {
      return;
    }

    pausePlayback();
    const result = await undoClip(activeClip.id);
    if (!result) {
      setEditorNotice("Nothing earlier to undo.");
      return;
    }

    applyHistoryResult(result);
    setEditorNotice("Reverted to the previous local state.");
  }

  async function handleRedo() {
    if (!activeClip) {
      return;
    }

    pausePlayback();
    const result = await redoClip(activeClip.id);
    if (!result) {
      setEditorNotice("Nothing newer to redo.");
      return;
    }

    applyHistoryResult(result);
    setEditorNotice("Re-applied the next local state.");
  }

  async function handleCommitClip(forceAccepted = false): Promise<boolean> {
    if (!activeClip || !projectDetail) {
      return false;
    }

    setIsCommittingClip(true);
    pausePlayback();

    let workingClip = activeClip;

    if (draftTranscript !== activeClip.transcript.text_current) {
      const transcriptClip = await updateClipTranscript(activeClip.id, draftTranscript);
      if (transcriptClip) {
        workingClip = transcriptClip;
        setProjectDetail((current) =>
          current ? replaceClipInProject(current, transcriptClip) : current,
        );
      }
    }

    const currentTagDraft = workingClip.tags.map((tag) => tag.name).join(", ");
    if (draftTags.trim() !== currentTagDraft.trim()) {
      const tagClip = await updateClipTags(workingClip.id, parseTagDraft(draftTags));
      if (tagClip) {
        workingClip = tagClip;
        setProjectDetail((current) => (current ? replaceClipInProject(current, tagClip) : current));
      }
    }

    if (forceAccepted && workingClip.review_status !== "accepted") {
      const statusClip = await updateClipStatus(workingClip.id, "accepted");
      if (!statusClip) {
        setIsCommittingClip(false);
        setEditorNotice("Could not mark clip as accepted before commit.");
        return false;
      }
      workingClip = statusClip;
      setProjectDetail((current) => (current ? replaceClipInProject(current, statusClip) : current));
    }

    const message =
      workingClip.review_status === "accepted"
        ? "Accepted clip snapshot"
        : "Manual review commit";

    const createdCommit = await commitClip(workingClip.id, message);
    setIsCommittingClip(false);

    if (!createdCommit) {
      setEditorNotice("Commit failed. Check the backend.");
      return false;
    }

    const committedClip: Clip = {
      ...workingClip,
      edit_state: "committed",
      updated_at: new Date().toISOString(),
    };
    setProjectDetail((current) => (current ? replaceClipInProject(current, committedClip) : current));
    setClipCommits((current) => ({
      ...current,
      [workingClip.id]: [...(current[workingClip.id] ?? []), createdCommit],
    }));
    setHistoryByClip((current) => ({
      ...current,
      [workingClip.id]: { can_undo: true, can_redo: false },
    }));
    setEditorNotice(`Committed clip snapshot: ${createdCommit.message}`);
    return true;
  }

  async function handleAcceptCommitNextAndPlay() {
    if (!activeClip || isCommittingClip || isApplyingEdit) {
      return;
    }

    const nextClipId = getNextClipId(activeClip.id);
    const committed = await handleCommitClip(true);
    if (!committed) {
      return;
    }

    if (!nextClipId) {
      setEditorNotice("Committed. No next clip in the current queue.");
      return;
    }

    shouldAutoPlayAfterClipChangeRef.current = true;
    handleClipSelect(nextClipId);
  }

  async function handleRejectNextAndPlay() {
    if (!activeClip || isCommittingClip || isApplyingEdit) {
      return;
    }

    const nextClipId = getNextClipId(activeClip.id);
    pausePlayback();
    const updatedClip = await updateClipStatus(activeClip.id, "rejected");
    if (!updatedClip) {
      setEditorNotice("Could not mark clip as rejected.");
      return;
    }

    updateCurrentClip(updatedClip);
    setEditorNotice("Marked clip as rejected.");

    if (!nextClipId) {
      return;
    }

    shouldAutoPlayAfterClipChangeRef.current = true;
    handleClipSelect(nextClipId);
  }

  async function handleTogglePlayback() {
    if (!activeClip || !waveSurferRef.current) {
      return;
    }

    const waveSurfer = waveSurferRef.current;
    const cursorTime = Number(waveSurfer.getCurrentTime().toFixed(2));
    const selectionDuration = normalizedSelectionEnd - normalizedSelectionStart;
    const hasSelection = selectionDuration > 0.01;

    if (isPlaying) {
      pausePlayback();
      setWaveSurferTime(playbackStartSeconds);
      return;
    }

    let nextStart = cursorTime;
    let nextEnd: number | undefined;

    const isAtOrPastEnd =
      cursorTime >= activeClip.duration_seconds - 0.01 ||
      playheadSeconds >= activeClip.duration_seconds - 0.01;

    if (hasSelection) {
      nextStart = normalizedSelectionStart;
      nextEnd = normalizedSelectionEnd;
    } else if (isAtOrPastEnd) {
      nextStart = Math.max(0, Math.min(playbackStartSeconds, activeClip.duration_seconds));
    }

    setWaveSurferTime(nextStart);
    setPlaybackStartSeconds(nextStart);

    try {
      await waveSurfer.play(nextStart, nextEnd);
      setIsPlaying(true);
      setEditorNotice(null);
    } catch {
      setEditorNotice("Audio preview could not start. Check the backend audio route.");
    }
  }

  async function handlePlayFromBeginning() {
    if (!activeClip || !waveSurferRef.current) {
      return;
    }

    const waveSurfer = waveSurferRef.current;
    pausePlayback();
    setSelectionStart(0);
    setSelectionEnd(0);
    setWaveSurferTime(0);
    setPlaybackStartSeconds(0);

    try {
      await waveSurfer.play(0);
      setIsPlaying(true);
      setEditorNotice(null);
    } catch {
      setEditorNotice("Audio preview could not start. Check the backend audio route.");
    }
  }

  async function handleSplitClip() {
    if (!activeClip) {
      return;
    }

    const splitAt =
      normalizedSelectionEnd > 0 && normalizedSelectionEnd < activeClip.duration_seconds
        ? normalizedSelectionEnd
        : Number((activeClip.duration_seconds / 2).toFixed(2));

    if (splitAt <= 0 || splitAt >= activeClip.duration_seconds) {
      setEditorNotice("Choose a valid split point inside the clip.");
      return;
    }

    setIsApplyingEdit(true);
    pausePlayback();
    const result = await splitClip(activeClip.id, splitAt);
    setIsApplyingEdit(false);

    if (!result) {
      setEditorNotice("Split failed. Check the backend response.");
      return;
    }

    setProjectDetail(result.project_detail);
    setActiveClipId(result.created_clip_ids[0] ?? null);
    setEditorNotice(`Split ${activeClip.id} into ${result.created_clip_ids.length} clips.`);
  }

  async function handleMergeClip() {
    if (!activeClip) {
      return;
    }

    setIsApplyingEdit(true);
    pausePlayback();
    const result = await mergeWithNextClip(activeClip.id);
    setIsApplyingEdit(false);

    if (!result) {
      setEditorNotice("Merge failed. The next clip may be incompatible.");
      return;
    }

    setProjectDetail(result.project_detail);
    setActiveClipId(result.created_clip_ids[0] ?? null);
    setEditorNotice(`Merged into ${result.created_clip_ids[0] ?? "a new clip"}.`);
  }

  function handleJumpToNextUnresolved() {
    if (!projectDetail) {
      return;
    }

    const unresolved = sortClipsForQueue(projectDetail.clips).filter(
      (clip) => clip.review_status !== "accepted" && clip.review_status !== "rejected",
    );

    if (unresolved.length === 0) {
      setEditorNotice("All clips are resolved.");
      return;
    }

    const currentIndex = unresolved.findIndex((clip) => clip.id === activeClip?.id);
    const nextClip = unresolved[(currentIndex + 1) % unresolved.length] ?? unresolved[0];
    handleClipSelect(nextClip.id);
  }

  const activeCommits = activeClip ? clipCommits[activeClip.id] ?? [] : [];
  const activeHistory = activeClip
    ? historyByClip[activeClip.id] ?? { can_undo: false, can_redo: false }
    : { can_undo: false, can_redo: false };
  const datasetStatusCounts = useMemo(() => {
    const counts: Record<ReviewStatus, number> = {
      candidate: 0,
      needs_attention: 0,
      in_review: 0,
      accepted: 0,
      rejected: 0,
    };
    const durations: Record<ReviewStatus, number> = {
      candidate: 0,
      needs_attention: 0,
      in_review: 0,
      accepted: 0,
      rejected: 0,
    };
    for (const clip of projectDetail?.clips ?? []) {
      counts[clip.review_status] += 1;
      durations[clip.review_status] += clip.duration_seconds;
    }
    return { counts, durations };
  }, [projectDetail?.clips]);
  const normalizedSelectionStart = Math.min(selectionStart, selectionEnd);
  const normalizedSelectionEnd = Math.max(selectionStart, selectionEnd);
  const hasActiveSelection = normalizedSelectionEnd - normalizedSelectionStart > 0.01;
  const acceptedRejectedRatio =
    datasetStatusCounts.durations.rejected > 0
      ? datasetStatusCounts.durations.accepted / datasetStatusCounts.durations.rejected
      : null;
  const totalDurationSeconds = projectDetail?.stats.total_duration_seconds ?? 0;
  const resolvedDurationSeconds =
    datasetStatusCounts.durations.accepted + datasetStatusCounts.durations.rejected;
  const smoothingSeconds = 10;
  const smoothedAcceptRate =
    resolvedDurationSeconds >= 0
      ? (datasetStatusCounts.durations.accepted + smoothingSeconds) /
        (resolvedDurationSeconds + smoothingSeconds * 2)
      : null;
  const predictedOutputSeconds =
    smoothedAcceptRate !== null
      ? Math.min(totalDurationSeconds, Math.max(0, totalDurationSeconds * smoothedAcceptRate))
      : null;
  const progressPercent =
    totalDurationSeconds > 0
      ? Math.min(100, Math.max(0, (resolvedDurationSeconds / totalDurationSeconds) * 100))
      : null;

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Speechcraft</p>
          <h1>{projectDetail?.project.name ?? "Loading Phase 1 Workspace"}</h1>
        </div>
        <div className="topbar-actions">
          <a className="status-pill route-link" href="/backend-test">
            Backend Test Route
          </a>
          <span className="status-pill">
            Export: {projectDetail?.project.export_status.replace(/_/g, " ") ?? "loading"}
          </span>
          <button
            className="primary-button"
            type="button"
            onClick={handleRunExport}
            disabled={!projectDetail || isRunningExport}
          >
            {isRunningExport ? "Rendering..." : "Run Export"}
          </button>
        </div>
      </header>

      <main className="workspace-grid">
        <aside className="clip-queue panel">
          <div className="clip-queue-tools">
            <input
              aria-label="Search clips"
              className="search-input"
              placeholder="Search clips"
              value={searchQuery}
              onChange={(event) => setSearchQuery(event.target.value)}
            />
            <div className="tag-filter-bar" ref={tagFilterRef}>
              <button
                type="button"
                className="tag-filter-trigger"
                onClick={() => setIsTagFilterMenuOpen((current) => !current)}
              >
                {selectedFilterTags.length > 0
                  ? `Tags (${selectedFilterTags.length})`
                  : "Filter Tags"}
              </button>
              <div className="tag-filter-current">
                {selectedFilterTags.length > 0 ? selectedFilterTags.join(", ") : "All tags"}
              </div>
              {isTagFilterMenuOpen ? (
                <div className="tag-filter-popover">
                  <ul className="tag-filter-list">
                    {availableFilterTags.map((tagName) => (
                      <li key={`filter-${tagName}`}>
                        <button
                          type="button"
                          className={`tag-filter-item ${selectedFilterTags.includes(tagName) ? "selected" : ""}`}
                          onClick={() => toggleFilterTag(tagName)}
                        >
                          <span>{tagName}</span>
                        </button>
                      </li>
                    ))}
                  </ul>
                  <div className="clip-list-meta">
                    <span>
                      {selectedFilterTags.length > 0
                        ? `Filtering: ${selectedFilterTags.join(", ")}`
                        : "No tag filter"}
                    </span>
                    {selectedFilterTags.length > 0 ? (
                      <button type="button" onClick={() => setSelectedFilterTags([])}>
                        Clear
                      </button>
                    ) : null}
                  </div>
                </div>
              ) : null}
            </div>
          </div>

          <div className="clip-list">
            {queueClips.map((clip, index) => (
              <button
                key={clip.id}
                className={`clip-list-item ${clip.id === activeClip?.id ? "active" : ""}`}
                type="button"
                onClick={() => handleClipSelect(clip.id)}
              >
                <div className="clip-list-row">
                  <strong>
                    <span className="order-pill">{index + 1}.</span>
                  </strong>
                  <span className={`review-chip status-${clip.review_status}`}>
                    {statusLabels[clip.review_status]}
                  </span>
                </div>
                <p>{clip.transcript.text_current}</p>
                <div className="clip-list-meta">
                  <span>{formatSeconds(clip.duration_seconds)}</span>
                  <span>{clip.edit_state}</span>
                </div>
              </button>
            ))}
          </div>
        </aside>

        <section className="editor-column">
          <section className="panel waveform-panel">
            <div className="panel-header">
              <div className="clip-editor-header-main">
                <p className="eyebrow">Clip Editor</p>
                {activeClip ? (
                  <div className="tag-list header-tag-list">
                    {activeClip.tags.length > 0 ? (
                      activeClip.tags.map((tag) => (
                        <span
                          key={`${activeClip.id}-top-${tag.name}`}
                          className="tag-pill"
                          style={{ backgroundColor: tag.color }}
                        >
                          {tag.name}
                        </span>
                      ))
                    ) : (
                      <span className="muted-copy">No tags</span>
                    )}
                  </div>
                ) : null}
              </div>
              {activeClip ? (
                <div className="clip-editor-header-actions">
                  <div className="metadata-strip">
                    <span>{formatSeconds(activeClip.duration_seconds)}</span>
                    <span>{activeClip.sample_rate / 1000} kHz</span>
                    <span>{activeClip.channels} ch</span>
                  </div>
                  <div className="editor-actions">
                    <button
                      className="primary-button"
                      type="button"
                      onClick={() => void handleAcceptCommitNextAndPlay()}
                      disabled={!activeClip || isCommittingClip}
                    >
                      Save Clip & Next
                    </button>
                    <button
                      className="primary-button"
                      type="button"
                      onClick={() => void handleRejectNextAndPlay()}
                      disabled={!activeClip || isCommittingClip || isApplyingEdit}
                    >
                      Reject Clip & Next
                    </button>
                  </div>
                </div>
              ) : null}
            </div>

            {editorNotice ? <p className="editor-notice">{editorNotice}</p> : null}

            {activeClip ? (
              <>
                <WaveformPane
                  audioUrl={buildClipAudioUrl(activeClip.id)}
                  durationSeconds={activeClip.duration_seconds}
                  peaks={null}
                  desiredCursorSeconds={playheadSeconds}
                  selectionStart={selectionStart}
                  selectionEnd={selectionEnd}
                  onSelectionChange={(start, end) => {
                    setSelectionStart(start);
                    setSelectionEnd(end);
                  }}
                  onCursorChange={(time) => setPlayheadSeconds(time)}
                  onHoverTimeChange={setHoverSeconds}
                  onReady={(instance) => {
                    waveSurferRef.current = instance;
                    applyPlaybackRate(instance, playbackRate);
                  }}
                  onPlayingChange={setIsPlaying}
                />

                <div className="waveform-second-scale" aria-hidden="true">
                  {Array.from(
                    { length: Math.floor(activeClip.duration_seconds) + 1 },
                    (_, second) => (
                      <span
                        key={`sec-tick-${activeClip.id}-${second}`}
                        className="waveform-second-tick"
                        style={{
                          left: `${(second / Math.max(activeClip.duration_seconds, 0.001)) * 100}%`,
                        }}
                      >
                        {second}s
                      </span>
                    ),
                  )}
                </div>

                <div className="timeline-strip transport-strip">
                  <span className="transport-pill">
                    {formatClipTimestamp(playheadSeconds)} /{" "}
                    {formatClipTimestamp(activeClip.duration_seconds)}
                  </span>
                  <span className="transport-meta">
                    {hoverSeconds !== null
                      ? `Hover ${formatClipTimestamp(hoverSeconds)}`
                      : "Hover --.--"}
                  </span>
                  <span className="transport-meta">
                    {hasActiveSelection
                      ? `Sel ${formatClipTimestamp(normalizedSelectionStart)}-${formatClipTimestamp(normalizedSelectionEnd)} (${formatClipTimestamp(
                          normalizedSelectionEnd - normalizedSelectionStart,
                        )})`
                      : "Sel none"}
                  </span>
                </div>

                <div className="editor-actions">
                  <button type="button" onClick={() => void handleTogglePlayback()}>
                    {isPlaying ? "Pause" : "Play"}
                  </button>
                  <button type="button" onClick={handleCyclePlaybackRate}>
                    Speed {playbackRate}x
                  </button>
                  <button type="button" onClick={handleUndo} disabled={!activeHistory.can_undo}>
                    Undo
                  </button>
                  <button type="button" onClick={handleRedo} disabled={!activeHistory.can_redo}>
                    Redo
                  </button>
                  <button type="button" onClick={handleSplitClip} disabled={isApplyingEdit}>
                    {isApplyingEdit ? "Applying..." : "Split Clip"}
                  </button>
                  <button type="button" onClick={handleMergeClip} disabled={isApplyingEdit}>
                    Merge Next Clip
                  </button>
                  {hasActiveSelection ? (
                    <button
                      type="button"
                      onClick={handleDeleteSelection}
                      disabled={isApplyingEdit}
                    >
                      Delete Selection
                    </button>
                  ) : null}
                  <button type="button" onClick={handleInsertSilence} disabled={isApplyingEdit}>
                    Insert Silence
                  </button>
                </div>
              </>
            ) : (
              <div className="empty-state">Select a clip to begin review.</div>
            )}
          </section>

          <section className="panel transcript-panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Transcript</p>
              </div>
            </div>

            <textarea
              ref={transcriptEditorRef}
              className="transcript-editor"
              rows={1}
              value={draftTranscript}
              onChange={(event) => {
                const editor = event.target;
                editor.style.height = "auto";
                editor.style.height = `${editor.scrollHeight}px`;
                setDraftTranscript(editor.value);
              }}
              placeholder="Transcript text"
            />

            <div className="selection-panel">
              <div className="selection-header">
                <strong>Tags</strong>
                <button className="primary-button" type="button" onClick={handleTagsSave}>
                  Save Tags
                </button>
              </div>
              <p className="muted-copy">
                Export uses clip status (`accepted`) + commit state. Tags are for filtering/QA.
              </p>
              <div className="tag-token-list">
                {draftTagEntries.length > 0 ? (
                  draftTagEntries.map((tag) => (
                    <button
                      key={`draft-${tag.name}`}
                      type="button"
                      className="tag-pill"
                      style={{ backgroundColor: tag.color }}
                      onClick={() => removeTagFromDraft(tag.name)}
                      title="Remove tag"
                    >
                      {tag.name} ×
                    </button>
                  ))
                ) : (
                  <span className="muted-copy">No tags on this clip yet.</span>
                )}
              </div>
              <div className="tag-input-row">
                <input
                  className="search-input tag-entry-input"
                  value={tagInputDraft}
                  onChange={(event) => setTagInputDraft(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === ",") {
                      event.preventDefault();
                      handleAddTagFromInput();
                    }
                  }}
                  placeholder="Add tag (press Enter)"
                />
                <button type="button" onClick={handleAddTagFromInput}>
                  Add
                </button>
              </div>
              <div className="tag-suggestion-wrap">
                {suggestedTagNames.map((tagName) => (
                  <button
                    key={`suggested-tag-${tagName}`}
                    type="button"
                    className="tag-suggestion-pill"
                    onClick={() => addTagToDraft(tagName)}
                  >
                    + {tagName}
                  </button>
                ))}
              </div>
            </div>

            <div className="transcript-footer">
              <span>
                Source: <strong>{activeClip?.transcript.source ?? "n/a"}</strong>
              </span>
              <span>
                Confidence:{" "}
                <strong>
                  {activeClip?.transcript.confidence
                    ? `${Math.round(activeClip.transcript.confidence * 100)}%`
                    : "n/a"}
                </strong>
              </span>
            </div>
          </section>
        </section>

        <aside className="inspector-column panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Inspector</p>
              <h2>Clip Review</h2>
            </div>
          </div>

          {activeClip ? (
            <>
              <div className="status-group">
                {queuePriorityOrder.map((status) => (
                  <button
                    key={status}
                    className={`status-button ${activeClip.review_status === status ? "selected" : ""}`}
                    type="button"
                    onClick={() => void handleStatusChange(status)}
                  >
                    {statusLabels[status]}
                  </button>
                ))}
              </div>

              <section className="inspector-block">
                <div className="stats-table">
                  <div className="stats-row stats-head">
                    <span>Status</span>
                    <span>Clips</span>
                    <span>Length</span>
                  </div>
                  <div className="stats-row">
                    <span>Total</span>
                    <span>{projectDetail?.stats.total_clips ?? 0}</span>
                    <span>{formatDurationCompact(projectDetail?.stats.total_duration_seconds ?? 0)}</span>
                  </div>
                  {queuePriorityOrder
                    .filter((status) => datasetStatusCounts.counts[status] > 0)
                    .map((status) => (
                      <div key={`dataset-stat-${status}`} className="stats-row">
                        <span>{statusLabels[status]}</span>
                        <span>{datasetStatusCounts.counts[status]}</span>
                        <span>{formatDurationCompact(datasetStatusCounts.durations[status])}</span>
                      </div>
                    ))}
                  <div className="stats-divider" />
                  <div className="stats-row">
                    <span>A/R Ratio</span>
                    <span>-</span>
                    <span>{acceptedRejectedRatio !== null ? acceptedRejectedRatio.toFixed(2) : "n/a"}</span>
                  </div>
                  <div className="stats-row">
                    <span>Predicted Size</span>
                    <span>-</span>
                    <span>
                      {predictedOutputSeconds !== null
                        ? formatDurationCompact(predictedOutputSeconds)
                        : "n/a"}
                    </span>
                  </div>
                  <div className="stats-divider" />
                  <div className="stats-row">
                    <span>Progress</span>
                    <span>-</span>
                    <span>{progressPercent !== null ? `${Math.round(progressPercent)}%` : "n/a"}</span>
                  </div>
                </div>
              </section>

              <section className="inspector-block">
                <h3>Edit History</h3>
                {activeClip.clip_edl.length > 0 ? (
                  <ul className="edl-list">
                    {activeClip.clip_edl.map((operation, index) => (
                      <li key={`${activeClip.id}-edl-${index}`}>
                        <strong>{operation.op}</strong>
                        {operation.range ? (
                          <span>
                            {" "}
                            {formatSeconds(operation.range.start_seconds)} to{" "}
                            {formatSeconds(operation.range.end_seconds)}
                          </span>
                        ) : null}
                        {operation.duration_seconds ? (
                          <span> {formatSeconds(operation.duration_seconds)}</span>
                        ) : null}
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="muted-copy">No per-clip edits yet.</p>
                )}
              </section>

              <section className="inspector-block">
                <h3>Commit History</h3>
                {activeCommits.length > 0 ? (
                  <div className="commit-list">
                    {[...activeCommits].reverse().map((commitEntry) => (
                      <div key={commitEntry.id} className="commit-card">
                        <div className="commit-row">
                          <strong>{commitEntry.message}</strong>
                          <span>{statusLabels[commitEntry.review_status_snapshot]}</span>
                        </div>
                        <p>{commitEntry.transcript_snapshot}</p>
                        <span className="commit-time">
                          {new Date(commitEntry.created_at).toLocaleString()}
                        </span>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="muted-copy">No commits yet. Use Commit Clip to save a milestone.</p>
                )}
              </section>

              <section className="inspector-block">
                <h3>Export Runs</h3>
                {exportRuns.length > 0 ? (
                  <div className="commit-list">
                    {[...exportRuns].reverse().map((run) => (
                      <div key={run.id} className="commit-card">
                        <div className="commit-row">
                          <strong>{run.id}</strong>
                          <span>{run.status}</span>
                        </div>
                        <p>{run.manifest_path}</p>
                        <span className="commit-time">
                          {run.accepted_clip_count} clip(s)
                          {run.completed_at ? ` • ${new Date(run.completed_at).toLocaleString()}` : ""}
                        </span>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="muted-copy">No export runs yet.</p>
                )}
              </section>

              <section className="inspector-block">
                <h3>Provenance</h3>
                <dl>
                  <div>
                    <dt>Source</dt>
                    <dd>{activeClip.source_file_id}</dd>
                  </div>
                  <div>
                    <dt>Working Asset</dt>
                    <dd>{activeClip.working_asset_id}</dd>
                  </div>
                  <div>
                    <dt>Original Range</dt>
                    <dd>
                      {formatSeconds(activeClip.original_start_time)} to{" "}
                      {formatSeconds(activeClip.original_end_time)}
                    </dd>
                  </div>
                  <div>
                    <dt>Edit State</dt>
                    <dd>{activeClip.edit_state}</dd>
                  </div>
                </dl>
              </section>
            </>
          ) : (
            <div className="empty-state">No clip selected.</div>
          )}
        </aside>
      </main>
    </div>
  );
}
