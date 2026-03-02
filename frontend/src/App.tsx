import {
  startTransition,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
} from "react";
import {
  appendClipEdlOperation,
  buildClipAudioUrl,
  commitClip,
  fetchClipCommits,
  fetchExportPreview,
  fetchProjectDetail,
  fetchProjectExports,
  fetchWaveformPeaks,
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
import type {
  Clip,
  ClipCommit,
  ClipHistoryResult,
  ExportPreview,
  ExportRun,
  ProjectDetail,
  ReviewStatus,
  WaveformPeaks,
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

type HistoryFlags = {
  can_undo: boolean;
  can_redo: boolean;
};

function formatSeconds(value: number): string {
  return `${value.toFixed(2)}s`;
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
  tagFilter: string,
  hideResolved: boolean,
): boolean {
  if (hideResolved && (clip.review_status === "accepted" || clip.review_status === "rejected")) {
    return false;
  }

  if (tagFilter && !clip.tags.some((tag) => tag.name.toLowerCase().includes(tagFilter))) {
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
  const [tagFilter, setTagFilter] = useState("");
  const [hideResolved, setHideResolved] = useState(false);
  const [selectionStart, setSelectionStart] = useState(0);
  const [selectionEnd, setSelectionEnd] = useState(0);
  const [playheadSeconds, setPlayheadSeconds] = useState(0);
  const [isPlaying, setIsPlaying] = useState(false);
  const [waveformPeaks, setWaveformPeaks] = useState<WaveformPeaks | null>(null);
  const [isWaveformLoading, setIsWaveformLoading] = useState(false);
  const [exportPreview, setExportPreview] = useState<ExportPreview | null>(null);
  const [exportRuns, setExportRuns] = useState<ExportRun[]>([]);
  const [isExportPreviewLoading, setIsExportPreviewLoading] = useState(false);
  const [isRunningExport, setIsRunningExport] = useState(false);
  const [clipCommits, setClipCommits] = useState<Record<string, ClipCommit[]>>({});
  const [historyByClip, setHistoryByClip] = useState<Record<string, HistoryFlags>>({});
  const [isCommittingClip, setIsCommittingClip] = useState(false);
  const [isApplyingEdit, setIsApplyingEdit] = useState(false);
  const [editorNotice, setEditorNotice] = useState<string | null>(null);
  const [dragMode, setDragMode] = useState<"selection" | "start-handle" | "end-handle" | null>(
    null,
  );
  const deferredSearch = useDeferredValue(searchQuery.trim().toLowerCase());
  const deferredTagFilter = useDeferredValue(tagFilter.trim().toLowerCase());
  const waveformRef = useRef<HTMLDivElement | null>(null);
  const audioRef = useRef<HTMLAudioElement | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function loadWorkspace() {
      const [detail, exports] = await Promise.all([
        fetchProjectDetail(),
        fetchProjectExports(),
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

  const queueClips = useMemo(() => {
    const clips = projectDetail?.clips ?? [];
    return sortClipsForQueue(clips).filter((clip) =>
      clipMatchesFilters(clip, deferredSearch, deferredTagFilter, hideResolved),
    );
  }, [projectDetail?.clips, deferredSearch, deferredTagFilter, hideResolved]);

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
    setSelectionStart(0);
    setSelectionEnd(Number(activeClip.duration_seconds.toFixed(2)));
    setPlayheadSeconds(0);
    setIsPlaying(false);
  }, [activeClip?.id]);

  useEffect(() => {
    if (!activeClip) {
      setWaveformPeaks(null);
      return;
    }

    let cancelled = false;
    setIsWaveformLoading(true);

    const requestedBins = 320;

    void fetchWaveformPeaks(activeClip.id, requestedBins).then((peaks) => {
      if (cancelled) {
        return;
      }

      setWaveformPeaks(peaks);
      setIsWaveformLoading(false);
    });

    return () => {
      cancelled = true;
    };
  }, [activeClip?.id]);

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

  useEffect(() => {
    if (!activeClip || !audioRef.current) {
      return;
    }

    const audio = audioRef.current;
    const nextUrl = buildClipAudioUrl(activeClip.id);
    if (audio.src !== nextUrl) {
      audio.src = nextUrl;
    }
    audio.currentTime = 0;
    audio.load();

    const handleTimeUpdate = () => {
      setPlayheadSeconds(Number(audio.currentTime.toFixed(2)));
    };

    const handleEnded = () => {
      setIsPlaying(false);
      setPlayheadSeconds(Number(activeClip.duration_seconds.toFixed(2)));
    };

    audio.addEventListener("timeupdate", handleTimeUpdate);
    audio.addEventListener("ended", handleEnded);

    return () => {
      audio.pause();
      audio.removeEventListener("timeupdate", handleTimeUpdate);
      audio.removeEventListener("ended", handleEnded);
    };
  }, [activeClip?.id, activeClip?.duration_seconds]);

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
    setExportPreview(null);
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
    setSelectionEnd(Number(result.clip.duration_seconds.toFixed(2)));
    setPlayheadSeconds(0);
    setExportPreview(null);
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

  function pausePlayback() {
    if (audioRef.current) {
      audioRef.current.pause();
    }
    setIsPlaying(false);
  }

  async function handleStatusChange(reviewStatus: ReviewStatus) {
    if (!activeClip) {
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

  async function handleTranscriptSave() {
    if (!activeClip) {
      return;
    }

    pausePlayback();
    const updatedClip = await updateClipTranscript(activeClip.id, draftTranscript);
    if (!updatedClip) {
      setEditorNotice("Transcript save failed. Check the backend.");
      return;
    }

    updateCurrentClip(updatedClip);
    setEditorNotice("Saved transcript edits.");
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

  async function handleExportPreview() {
    if (!projectDetail) {
      return;
    }

    setIsExportPreviewLoading(true);
    const preview = await fetchExportPreview(projectDetail.project.id, projectDetail.clips);
    setExportPreview(preview);
    setIsExportPreviewLoading(false);
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

    const preview = await fetchExportPreview(detail.project.id, detail.clips);
    setExportPreview(preview);
    setEditorNotice(`Export completed: ${result.accepted_clip_count} clip(s) rendered.`);
  }

  async function handleDeleteSelection() {
    if (!activeClip) {
      return;
    }

    const start = Math.min(selectionStart, selectionEnd);
    const end = Math.max(selectionStart, selectionEnd);

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
    setSelectionStart(0);
    setSelectionEnd(Number(updatedClip.duration_seconds.toFixed(2)));
    setPlayheadSeconds(Math.min(start, updatedClip.duration_seconds));
    if (audioRef.current) {
      audioRef.current.currentTime = Math.min(start, updatedClip.duration_seconds);
    }
    setEditorNotice("Deleted the selected region.");
  }

  async function handleInsertSilence() {
    if (!activeClip) {
      return;
    }

    const start = Math.min(selectionStart, selectionEnd);
    const end = Math.max(selectionStart, selectionEnd);
    const silenceDuration = Number((Math.max(end - start, 0) || 0.25).toFixed(2));

    setIsApplyingEdit(true);
    pausePlayback();
    const updatedClip = await appendClipEdlOperation(activeClip.id, {
      op: "insert_silence",
      duration_seconds: silenceDuration,
    });
    setIsApplyingEdit(false);

    if (!updatedClip) {
      setEditorNotice("Insert silence failed. Check the backend.");
      return;
    }

    updateCurrentClip(updatedClip);
    setSelectionStart(0);
    setSelectionEnd(Number(updatedClip.duration_seconds.toFixed(2)));
    setPlayheadSeconds(Math.min(start, updatedClip.duration_seconds));
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

  async function handleCommitClip() {
    if (!activeClip || !projectDetail) {
      return;
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

    const message =
      workingClip.review_status === "accepted"
        ? "Accepted clip snapshot"
        : "Manual review commit";

    const createdCommit = await commitClip(workingClip.id, message);
    setIsCommittingClip(false);

    if (!createdCommit) {
      setEditorNotice("Commit failed. Check the backend.");
      return;
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
    setExportPreview(null);
    setHistoryByClip((current) => ({
      ...current,
      [workingClip.id]: { can_undo: true, can_redo: false },
    }));
    setEditorNotice(`Committed clip snapshot: ${createdCommit.message}`);
  }

  function getPointerTime(
    clientX: number,
    rect: DOMRect,
    durationSeconds: number,
    startRatio: number,
    windowRatio: number,
  ): number {
    const ratio = Math.min(Math.max((clientX - rect.left) / rect.width, 0), 1);
    const absoluteRatio = startRatio + ratio * windowRatio;
    return Number((absoluteRatio * durationSeconds).toFixed(2));
  }

  function setAudioCurrentTime(nextTime: number) {
    if (audioRef.current) {
      audioRef.current.currentTime = nextTime;
    }
    setPlayheadSeconds(nextTime);
  }

  function handleWaveformPointerDown(event: ReactPointerEvent<HTMLDivElement>) {
    if (!activeClip) {
      return;
    }

    pausePlayback();
    const rect = event.currentTarget.getBoundingClientRect();
    const nextTime = getPointerTime(
      event.clientX,
      rect,
      activeClip.duration_seconds,
      visibleStartRatio,
      visibleWindowRatio,
    );
    event.currentTarget.setPointerCapture(event.pointerId);
    setSelectionStart(nextTime);
    setSelectionEnd(nextTime);
    setAudioCurrentTime(nextTime);
    setDragMode("selection");
  }

  function handleWaveformPointerMove(event: ReactPointerEvent<HTMLDivElement>) {
    if (!activeClip || dragMode !== "selection") {
      return;
    }

    const rect = event.currentTarget.getBoundingClientRect();
    const nextTime = getPointerTime(
      event.clientX,
      rect,
      activeClip.duration_seconds,
      visibleStartRatio,
      visibleWindowRatio,
    );
    setSelectionEnd(nextTime);
    setAudioCurrentTime(nextTime);
  }

  function handleWaveformPointerUp(event: ReactPointerEvent<HTMLDivElement>) {
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
    setDragMode(null);
  }

  function handleSelectionHandlePointerDown(
    which: "start-handle" | "end-handle",
    event: ReactPointerEvent<HTMLDivElement>,
  ) {
    event.stopPropagation();
    pausePlayback();
    event.currentTarget.setPointerCapture(event.pointerId);
    setDragMode(which);
  }

  function handleSelectionHandlePointerMove(event: ReactPointerEvent<HTMLDivElement>) {
    if (!activeClip || !dragMode || !waveformRef.current) {
      return;
    }

    const isStartHandle = dragMode === "start-handle";
    const isEndHandle = dragMode === "end-handle";

    if (!isStartHandle && !isEndHandle) {
      return;
    }

    const rect = waveformRef.current.getBoundingClientRect();
    const nextTime = getPointerTime(
      event.clientX,
      rect,
      activeClip.duration_seconds,
      visibleStartRatio,
      visibleWindowRatio,
    );

    if (isStartHandle) {
      setSelectionStart(nextTime);
    }

    if (isEndHandle) {
      setSelectionEnd(nextTime);
    }

    setAudioCurrentTime(nextTime);
  }

  function handleSelectionHandlePointerUp(event: ReactPointerEvent<HTMLDivElement>) {
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
    setDragMode(null);
  }

  async function handleTogglePlayback() {
    if (!activeClip || !audioRef.current) {
      return;
    }

    const audio = audioRef.current;

    if (isPlaying) {
      pausePlayback();
      return;
    }

    if (playheadSeconds >= activeClip.duration_seconds) {
      const resetPoint = normalizedSelectionStart > 0 ? normalizedSelectionStart : 0;
      audio.currentTime = resetPoint;
      setPlayheadSeconds(resetPoint);
    }

    try {
      await audio.play();
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
    setExportPreview(null);
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
    setExportPreview(null);
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

  const stats = projectDetail?.stats;
  const activeCommits = activeClip ? clipCommits[activeClip.id] ?? [] : [];
  const activeHistory = activeClip
    ? historyByClip[activeClip.id] ?? { can_undo: false, can_redo: false }
    : { can_undo: false, can_redo: false };
  const visibleWindowRatio = 1;
  const visibleStartRatio = 0;
  const visibleStartSeconds =
    activeClip && activeClip.duration_seconds > 0
      ? activeClip.duration_seconds * visibleStartRatio
      : 0;
  const visibleEndSeconds =
    activeClip && activeClip.duration_seconds > 0
      ? activeClip.duration_seconds * Math.min(visibleStartRatio + visibleWindowRatio, 1)
      : 0;
  const normalizedSelectionStart = Math.min(selectionStart, selectionEnd);
  const normalizedSelectionEnd = Math.max(selectionStart, selectionEnd);
  const visibleSelectionStart = Math.max(normalizedSelectionStart, visibleStartSeconds);
  const visibleSelectionEnd = Math.min(normalizedSelectionEnd, visibleEndSeconds);
  const visibleSelectionDuration = Math.max(visibleSelectionEnd - visibleSelectionStart, 0);
  const selectionOffsetPercent =
    activeClip && activeClip.duration_seconds > 0
      ? ((visibleSelectionStart / activeClip.duration_seconds - visibleStartRatio) / visibleWindowRatio) * 100
      : 0;
  const selectionWidthPercent =
    activeClip && activeClip.duration_seconds > 0
      ? Math.max(
          ((visibleSelectionDuration / activeClip.duration_seconds) / visibleWindowRatio) * 100,
          visibleSelectionDuration > 0 ? 0.8 : 0,
        )
      : 0;
  const selectionStartInView = normalizedSelectionStart >= visibleStartSeconds && normalizedSelectionStart <= visibleEndSeconds;
  const selectionEndInView = normalizedSelectionEnd >= visibleStartSeconds && normalizedSelectionEnd <= visibleEndSeconds;
  const selectionStartPercent =
    activeClip && activeClip.duration_seconds > 0
      ? (((normalizedSelectionStart / activeClip.duration_seconds) - visibleStartRatio) / visibleWindowRatio) * 100
      : 0;
  const selectionEndPercent =
    activeClip && activeClip.duration_seconds > 0
      ? (((normalizedSelectionEnd / activeClip.duration_seconds) - visibleStartRatio) / visibleWindowRatio) * 100
      : 0;
  const playheadPercent =
    activeClip && activeClip.duration_seconds > 0
      ? Math.min(
          Math.max(
            (((playheadSeconds / activeClip.duration_seconds) - visibleStartRatio) / visibleWindowRatio) * 100,
            0,
          ),
          100,
        )
      : 0;
  const waveformBars = useMemo(() => {
    const peaks = waveformPeaks?.peaks ?? [];
    if (peaks.length === 0) {
      return [];
    }

    const startIndex = Math.floor(peaks.length * visibleStartRatio);
    const endIndex = Math.max(
      startIndex + 1,
      Math.ceil(peaks.length * Math.min(visibleStartRatio + visibleWindowRatio, 1)),
    );

    return peaks.slice(startIndex, endIndex);
  }, [visibleStartRatio, visibleWindowRatio, waveformPeaks?.peaks]);
  const visibleQueueCount = queueClips.length;

  return (
    <div className="app-shell">
      <audio ref={audioRef} preload="metadata" />

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
            onClick={handleExportPreview}
            disabled={!projectDetail || isExportPreviewLoading}
          >
            {isExportPreviewLoading ? "Building Preview..." : "Preview Export"}
          </button>
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
          <div className="panel-header">
            <div>
              <p className="eyebrow">Queue</p>
              <h2>Clip Review</h2>
            </div>
            <input
              aria-label="Search clips"
              className="search-input"
              placeholder="Search clips"
              value={searchQuery}
              onChange={(event) => setSearchQuery(event.target.value)}
            />
          </div>

          <div className="stats-grid">
            <div className="stat-card">
              <span>Total</span>
              <strong>{stats?.total_clips ?? 0}</strong>
            </div>
            <div className="stat-card">
              <span>Accepted</span>
              <strong>{stats?.accepted_clips ?? 0}</strong>
            </div>
            <div className="stat-card">
              <span>Needs Attention</span>
              <strong>{stats?.needs_attention_clips ?? 0}</strong>
            </div>
            <div className="stat-card">
              <span>Visible</span>
              <strong>{visibleQueueCount}</strong>
            </div>
          </div>

          <div className="selection-panel">
            <label>
              Filter Tag
              <input
                className="search-input"
                value={tagFilter}
                onChange={(event) => setTagFilter(event.target.value)}
                placeholder="Tag name"
              />
            </label>
            <div className="editor-actions">
              <button type="button" onClick={() => setHideResolved((current) => !current)}>
                {hideResolved ? "Show Resolved" : "Hide Resolved"}
              </button>
              <button type="button" onClick={handleJumpToNextUnresolved}>
                Next Unresolved
              </button>
            </div>
          </div>

          <div className="clip-list">
            {queueClips.map((clip) => (
              <button
                key={clip.id}
                className={`clip-list-item ${clip.id === activeClip?.id ? "active" : ""}`}
                type="button"
                onClick={() => handleClipSelect(clip.id)}
              >
                <div className="clip-list-row">
                  <strong>
                    <span className="order-pill">#{clip.order_index}</span> {clip.id}
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
              <div>
                <p className="eyebrow">Clip Editor</p>
                <h2>{activeClip?.id ?? "No clip selected"}</h2>
              </div>
              {activeClip ? (
                <div className="metadata-strip">
                  <span>{formatSeconds(activeClip.duration_seconds)}</span>
                  <span>{activeClip.sample_rate / 1000} kHz</span>
                  <span>{activeClip.channels} ch</span>
                </div>
              ) : null}
            </div>

            {editorNotice ? <p className="editor-notice">{editorNotice}</p> : null}

            {activeClip ? (
              <>
                <div
                  ref={waveformRef}
                  className="waveform-stage"
                  aria-label="Interactive waveform selection"
                  onPointerDown={handleWaveformPointerDown}
                  onPointerMove={handleWaveformPointerMove}
                  onPointerUp={handleWaveformPointerUp}
                  onPointerCancel={handleWaveformPointerUp}
                >
                  {visibleSelectionDuration > 0 ? (
                    <div
                      className="selection-overlay"
                      style={{
                        left: `${selectionOffsetPercent}%`,
                        width: `${selectionWidthPercent}%`,
                      }}
                    />
                  ) : null}
                  <div
                    className="playhead"
                    style={{
                      left: `${playheadPercent}%`,
                    }}
                  />
                  {selectionStartInView ? (
                    <div
                      className="selection-handle start"
                      style={{ left: `${selectionStartPercent}%` }}
                      onPointerDown={(event) =>
                        handleSelectionHandlePointerDown("start-handle", event)
                      }
                      onPointerMove={handleSelectionHandlePointerMove}
                      onPointerUp={handleSelectionHandlePointerUp}
                      onPointerCancel={handleSelectionHandlePointerUp}
                    />
                  ) : null}
                  {selectionEndInView ? (
                    <div
                      className="selection-handle end"
                      style={{ left: `${selectionEndPercent}%` }}
                      onPointerDown={(event) =>
                        handleSelectionHandlePointerDown("end-handle", event)
                      }
                      onPointerMove={handleSelectionHandlePointerMove}
                      onPointerUp={handleSelectionHandlePointerUp}
                      onPointerCancel={handleSelectionHandlePointerUp}
                    />
                  ) : null}
                  {waveformBars.length > 0 ? (
                    waveformBars.map((peak, index) => (
                      <span
                        key={`${activeClip.id}-${index}`}
                        className="wave-bar"
                        style={{ height: `${14 + peak * 74}%` }}
                      />
                    ))
                  ) : (
                    <div className="empty-state">
                      {isWaveformLoading ? "Loading waveform..." : "No waveform data yet."}
                    </div>
                  )}
                </div>

                <div className="timeline-strip">
                  <span>{formatSeconds(activeClip.original_start_time + visibleStartSeconds)}</span>
                  <span>Visible Window</span>
                  <span>{formatSeconds(activeClip.original_start_time + visibleEndSeconds)}</span>
                </div>

                <div className="editor-actions">
                  <button type="button" onClick={() => void handleTogglePlayback()}>
                    {isPlaying ? "Pause" : "Play"}
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
                  <button type="button" onClick={handleDeleteSelection} disabled={isApplyingEdit}>
                    Delete Selection
                  </button>
                  <button type="button" onClick={handleInsertSilence} disabled={isApplyingEdit}>
                    Insert Silence
                  </button>
                  <button type="button" onClick={handleCommitClip} disabled={isCommittingClip}>
                    {isCommittingClip ? "Committing..." : "Commit Clip"}
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
                <h2>Manual Review</h2>
              </div>
              <button className="primary-button" type="button" onClick={handleTranscriptSave}>
                Save Transcript
              </button>
            </div>

            <textarea
              className="transcript-editor"
              value={draftTranscript}
              onChange={(event) => setDraftTranscript(event.target.value)}
              placeholder="Transcript text"
            />

            <div className="selection-panel">
              <div className="selection-header">
                <strong>Tags</strong>
                <button type="button" onClick={handleTagsSave}>
                  Save Tags
                </button>
              </div>
              <label>
                Comma-separated tags
                <input
                  className="search-input"
                  value={draftTags}
                  onChange={(event) => setDraftTags(event.target.value)}
                  placeholder="noisy, clipped_end, recheck"
                />
              </label>
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
              <h2>Clip State</h2>
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

              <section className="inspector-block">
                <h3>Tags</h3>
                <div className="tag-list">
                  {activeClip.tags.length > 0 ? (
                    activeClip.tags.map((tag) => (
                      <span
                        key={`${activeClip.id}-${tag.name}`}
                        className="tag-pill"
                        style={{ backgroundColor: tag.color }}
                      >
                        {tag.name}
                      </span>
                    ))
                  ) : (
                    <span className="muted-copy">No tags saved yet.</span>
                  )}
                </div>
              </section>

              <section className="inspector-block">
                <h3>EDL Summary</h3>
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
                <h3>Export Preview</h3>
                {exportPreview ? (
                  <div className="export-preview">
                    <p className="muted-copy">
                      {exportPreview.accepted_clip_count} committed accepted clip
                      {exportPreview.accepted_clip_count === 1 ? "" : "s"} ready
                    </p>
                    <p className="manifest-path">{exportPreview.manifest_path}</p>
                    {exportPreview.lines.length > 0 ? (
                      <pre className="manifest-preview">{exportPreview.lines.join("\n")}</pre>
                    ) : (
                      <p className="muted-copy">
                        No export-eligible clips yet. Accepted clips must also be committed.
                      </p>
                    )}
                  </div>
                ) : (
                  <p className="muted-copy">Generate a preview to inspect the next `.list` export.</p>
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
            </>
          ) : (
            <div className="empty-state">No clip selected.</div>
          )}
        </aside>
      </main>
    </div>
  );
}
