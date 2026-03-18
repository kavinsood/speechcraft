import { useEffect, useMemo, useRef, useState } from "react";
import type WaveSurfer from "wavesurfer.js";
import WaveformPane from "../WaveformPane";
import { fetchWaveformPeaks } from "../api";
import type { ReviewStatus, Slice, WaveformPeaks } from "../types";
import WorkspaceStatePanel from "./WorkspaceStatePanel";
import {
  formatClipTimestamp,
  formatSeconds,
  getAlignmentConfidence,
  getAlignmentSource,
  getRedoTarget,
  getSliceDuration,
  getSliceTranscriptText,
  parseTagDraft,
} from "./workspace-helpers";

type WorkspacePhase = "loading" | "error" | "empty" | "ready";

type EditorPaneProps = {
  workspacePhase: WorkspacePhase;
  workspaceError: string | null;
  workspaceEmptyMessage: string | null;
  activeClip: Slice | null;
  activeClipAudioUrl: string | null;
  canUndo: boolean;
  canRedo: boolean;
  allClipTagNames: string[];
  getNextClipId: (currentClipId: string) => string | null;
  onSelectClip: (clipId: string) => void;
  onRetryLoad: () => void;
  onSaveTranscript: (clipId: string, text: string) => Promise<Slice>;
  onSaveTags: (clipId: string, tags: { name: string; color: string }[]) => Promise<Slice>;
  onUpdateStatus: (clipId: string, status: ReviewStatus) => Promise<Slice>;
  onAppendEdlOperation: (
    clipId: string,
    payload: {
      op: string;
      range?: { start_seconds: number; end_seconds: number } | null;
      duration_seconds?: number | null;
    },
  ) => Promise<Slice>;
  onUndo: (clipId: string) => Promise<Slice>;
  onRedo: (clipId: string) => Promise<Slice>;
  onSplitClip: (clipId: string, splitAtSeconds: number) => Promise<Slice[]>;
  onMergeClip: (clipId: string) => Promise<Slice[]>;
  onRunClipLabModel: (clipId: string, generatorModel: string) => Promise<Slice>;
};

export default function EditorPane({
  workspacePhase,
  workspaceError,
  workspaceEmptyMessage,
  activeClip,
  activeClipAudioUrl,
  canUndo,
  canRedo,
  allClipTagNames,
  getNextClipId,
  onSelectClip,
  onRetryLoad,
  onSaveTranscript,
  onSaveTags,
  onUpdateStatus,
  onAppendEdlOperation,
  onUndo,
  onRedo,
  onSplitClip,
  onMergeClip,
  onRunClipLabModel,
}: EditorPaneProps) {
  const transcriptEditorRef = useRef<HTMLTextAreaElement | null>(null);
  const waveSurferRef = useRef<WaveSurfer | null>(null);
  const shouldAutoPlayAfterClipChangeRef = useRef(false);
  const playFromBeginningActionRef = useRef<() => Promise<void>>(async () => {});
  const togglePlaybackActionRef = useRef<() => Promise<void>>(async () => {});
  const rejectNextActionRef = useRef<() => Promise<void>>(async () => {});
  const acceptNextActionRef = useRef<() => Promise<void>>(async () => {});
  const [draftTranscript, setDraftTranscript] = useState("");
  const [draftTags, setDraftTags] = useState("");
  const [tagInputDraft, setTagInputDraft] = useState("");
  const [selectionStart, setSelectionStart] = useState(0);
  const [selectionEnd, setSelectionEnd] = useState(0);
  const [playheadSeconds, setPlayheadSeconds] = useState(0);
  const [hoverSeconds, setHoverSeconds] = useState<number | null>(null);
  const [playbackStartSeconds, setPlaybackStartSeconds] = useState(0);
  const [playbackRate, setPlaybackRate] = useState(1);
  const [isPlaying, setIsPlaying] = useState(false);
  const [isSavingSlice, setIsSavingSlice] = useState(false);
  const [isApplyingEdit, setIsApplyingEdit] = useState(false);
  const [isRunningModel, setIsRunningModel] = useState(false);
  const [editorNotice, setEditorNotice] = useState<string | null>(null);
  const [waveformPeaks, setWaveformPeaks] = useState<WaveformPeaks | null>(null);
  const [waveformError, setWaveformError] = useState<string | null>(null);

  const activeDuration = activeClip ? getSliceDuration(activeClip) : 0;
  const draftTagEntries = useMemo(() => parseTagDraft(draftTags), [draftTags]);
  const suggestedTagNames = useMemo(() => {
    const selected = new Set(draftTagEntries.map((tag) => tag.name.toLowerCase()));
    return allClipTagNames
      .filter((tagName) => !selected.has(tagName.toLowerCase()))
      .slice(0, 12);
  }, [allClipTagNames, draftTagEntries]);
  const normalizedSelectionStart = Math.min(selectionStart, selectionEnd);
  const normalizedSelectionEnd = Math.max(selectionStart, selectionEnd);
  const hasActiveSelection = normalizedSelectionEnd - normalizedSelectionStart > 0.01;

  useEffect(() => {
    if (!activeClip) {
      return;
    }

    setDraftTranscript(getSliceTranscriptText(activeClip));
    setDraftTags(activeClip.tags.map((tag) => tag.name).join(", "));
    setTagInputDraft("");
    setSelectionStart(0);
    setSelectionEnd(0);
    setPlayheadSeconds(0);
    setHoverSeconds(null);
    setPlaybackStartSeconds(0);
    setIsPlaying(false);
    setEditorNotice(null);
    setWaveformError(null);

    if (shouldAutoPlayAfterClipChangeRef.current) {
      shouldAutoPlayAfterClipChangeRef.current = false;
      window.setTimeout(() => {
        void handleTogglePlayback();
      }, 180);
    }
  }, [activeClip?.id]);

  useEffect(() => {
    if (!activeClip) {
      setWaveformPeaks(null);
      return;
    }

    let cancelled = false;
    setWaveformPeaks(null);
    setWaveformError(null);

    void (async () => {
      try {
        const nextPeaks = await fetchWaveformPeaks(activeClip.id, 960);
        if (cancelled) {
          return;
        }
        setWaveformPeaks(nextPeaks);
      } catch (error) {
        if (cancelled) {
          return;
        }
        const message =
          error instanceof Error ? error.message : "Waveform peaks failed to load for this slice.";
        console.error(message);
        setWaveformError(message);
        setWaveformPeaks(null);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [activeClip?.id, activeClip?.active_commit_id, activeClip?.active_variant_id]);

  useEffect(() => {
    const editor = transcriptEditorRef.current;
    if (!editor) {
      return;
    }
    editor.style.height = "auto";
    editor.style.height = `${editor.scrollHeight}px`;
  }, [draftTranscript, activeClip?.id]);

  function pausePlayback() {
    waveSurferRef.current?.pause();
    setIsPlaying(false);
  }

  function applyPlaybackRate(instance: WaveSurfer | null, rate: number) {
    if (!instance) {
      return;
    }
    (instance as unknown as { setPlaybackRate: (value: number, preservePitch?: boolean) => void }).setPlaybackRate(rate, true);
  }

  function setWaveSurferTime(nextTime: number) {
    const waveSurfer = waveSurferRef.current;
    if (!waveSurfer || activeDuration <= 0) {
      return;
    }

    const clamped = Math.max(0, Math.min(nextTime, activeDuration));
    waveSurfer.seekTo(clamped / activeDuration);
    setPlayheadSeconds(Number(clamped.toFixed(2)));
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
    setDraftTags([...currentTags, normalized].join(", "));
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

  function handleCyclePlaybackRate() {
    const nextRates = [0.5, 0.75, 1, 1.25, 1.5, 2] as const;
    const currentIndex = nextRates.indexOf(playbackRate as (typeof nextRates)[number]);
    const nextRate = nextRates[(currentIndex + 1) % nextRates.length];
    setPlaybackRate(nextRate);
    applyPlaybackRate(waveSurferRef.current, nextRate);
  }

  async function persistDrafts(): Promise<Slice | null> {
    if (!activeClip) {
      return null;
    }

    let workingSlice = activeClip;
    if (draftTranscript !== getSliceTranscriptText(activeClip)) {
      workingSlice = await onSaveTranscript(activeClip.id, draftTranscript);
    }

    const currentTagDraft = workingSlice.tags.map((tag) => tag.name).join(", ");
    if (draftTags.trim() !== currentTagDraft.trim()) {
      workingSlice = await onSaveTags(workingSlice.id, parseTagDraft(draftTags));
      setDraftTags(workingSlice.tags.map((tag) => tag.name).join(", "));
    }

    return workingSlice;
  }

  async function handleSaveSlice(forceStatus?: ReviewStatus): Promise<Slice | null> {
    if (!activeClip) {
      return null;
    }

    setIsSavingSlice(true);
    pausePlayback();

    try {
      let workingSlice = await persistDrafts();
      if (!workingSlice) {
        return null;
      }
      if (forceStatus && workingSlice.status !== forceStatus) {
        workingSlice = await onUpdateStatus(workingSlice.id, forceStatus);
      }
      setEditorNotice("Saved slice state.");
      return workingSlice;
    } catch (error) {
      setEditorNotice(error instanceof Error ? error.message : "Save failed.");
      return null;
    } finally {
      setIsSavingSlice(false);
    }
  }

  async function handleAcceptNextAndPlay() {
    if (!activeClip || isSavingSlice || isApplyingEdit) {
      return;
    }

    const nextClipId = getNextClipId(activeClip.id);
    const savedSlice = await handleSaveSlice("accepted");
    if (!savedSlice) {
      return;
    }

    if (!nextClipId) {
      setEditorNotice("Saved. No next slice in the current queue.");
      return;
    }

    shouldAutoPlayAfterClipChangeRef.current = true;
    onSelectClip(nextClipId);
  }

  async function handleRejectNextAndPlay() {
    if (!activeClip || isSavingSlice || isApplyingEdit) {
      return;
    }

    const nextClipId = getNextClipId(activeClip.id);
    const savedSlice = await handleSaveSlice("rejected");
    if (!savedSlice) {
      return;
    }

    if (!nextClipId) {
      return;
    }

    shouldAutoPlayAfterClipChangeRef.current = true;
    onSelectClip(nextClipId);
  }

  async function handleTogglePlayback() {
    if (!activeClip || !waveSurferRef.current) {
      return;
    }

    const waveSurfer = waveSurferRef.current;
    const cursorTime = Number(waveSurfer.getCurrentTime().toFixed(2));
    const selectionDuration = normalizedSelectionEnd - normalizedSelectionStart;

    if (isPlaying) {
      pausePlayback();
      setWaveSurferTime(playbackStartSeconds);
      return;
    }

    let nextStart = cursorTime;
    let nextEnd: number | undefined;

    const isAtOrPastEnd = cursorTime >= activeDuration - 0.01 || playheadSeconds >= activeDuration - 0.01;

    if (selectionDuration > 0.01) {
      nextStart = normalizedSelectionStart;
      nextEnd = normalizedSelectionEnd;
    } else if (isAtOrPastEnd) {
      nextStart = Math.max(0, Math.min(playbackStartSeconds, activeDuration));
    }

    setWaveSurferTime(nextStart);
    setPlaybackStartSeconds(nextStart);

    try {
      await waveSurfer.play(nextStart, nextEnd);
      setIsPlaying(true);
      setEditorNotice(null);
    } catch {
      setEditorNotice("Audio preview could not start. Check the active variant media route.");
    }
  }

  async function handlePlayFromBeginning() {
    if (!activeClip || !waveSurferRef.current) {
      return;
    }

    pausePlayback();
    setSelectionStart(0);
    setSelectionEnd(0);
    setWaveSurferTime(0);
    setPlaybackStartSeconds(0);

    try {
      await waveSurferRef.current.play(0);
      setIsPlaying(true);
      setEditorNotice(null);
    } catch {
      setEditorNotice("Audio preview could not start. Check the active variant media route.");
    }
  }

  playFromBeginningActionRef.current = handlePlayFromBeginning;
  togglePlaybackActionRef.current = handleTogglePlayback;
  rejectNextActionRef.current = handleRejectNextAndPlay;
  acceptNextActionRef.current = handleAcceptNextAndPlay;

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (target && (target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable)) {
        return;
      }

      if (event.ctrlKey || event.metaKey || event.altKey || event.repeat) {
        return;
      }

      if (event.code === "Space") {
        event.preventDefault();
        if (event.shiftKey) {
          void playFromBeginningActionRef.current();
          return;
        }
        void togglePlaybackActionRef.current();
        return;
      }

      if (event.code === "Enter") {
        event.preventDefault();
        if (event.shiftKey) {
          void rejectNextActionRef.current();
          return;
        }
        void acceptNextActionRef.current();
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, []);

  async function handleDeleteSelection() {
    if (!activeClip) {
      return;
    }

    const start = Math.max(0, Math.min(Math.min(selectionStart, selectionEnd), activeDuration));
    let end = Math.max(0, Math.min(Math.max(selectionStart, selectionEnd), activeDuration));
    if (activeDuration - end <= 0.03) {
      end = activeDuration;
    }

    if (end <= start) {
      setEditorNotice("Select a non-zero region before deleting.");
      return;
    }

    setIsApplyingEdit(true);
    pausePlayback();
    try {
      const updated = await onAppendEdlOperation(activeClip.id, {
        op: "delete_range",
        range: { start_seconds: start, end_seconds: end },
      });
      const nextTime = Math.min(start, getSliceDuration(updated));
      setSelectionStart(nextTime);
      setSelectionEnd(nextTime);
      setPlayheadSeconds(nextTime);
      setWaveSurferTime(nextTime);
      setEditorNotice("Deleted the selected region.");
    } catch (error) {
      setEditorNotice(error instanceof Error ? error.message : "Delete failed.");
    } finally {
      setIsApplyingEdit(false);
    }
  }

  async function handleInsertSilence() {
    if (!activeClip) {
      return;
    }

    const cursorTime = waveSurferRef.current
      ? Number(waveSurferRef.current.getCurrentTime().toFixed(2))
      : playheadSeconds;
    const insertAt = Math.max(0, Math.min(cursorTime, activeDuration));

    setIsApplyingEdit(true);
    pausePlayback();
    try {
      const updated = await onAppendEdlOperation(activeClip.id, {
        op: "insert_silence",
        range: { start_seconds: insertAt, end_seconds: insertAt },
        duration_seconds: 0.2,
      });
      const nextTime = Math.min(insertAt, getSliceDuration(updated));
      setSelectionStart(nextTime);
      setSelectionEnd(nextTime);
      setPlayheadSeconds(nextTime);
      setEditorNotice(`Inserted ${formatSeconds(0.2)} of silence.`);
    } catch (error) {
      setEditorNotice(error instanceof Error ? error.message : "Insert silence failed.");
    } finally {
      setIsApplyingEdit(false);
    }
  }

  async function handleUndo() {
    if (!activeClip) {
      return;
    }

    pausePlayback();
    try {
      const updated = await onUndo(activeClip.id);
      setDraftTranscript(getSliceTranscriptText(updated));
      setDraftTags(updated.tags.map((tag) => tag.name).join(", "));
      setSelectionStart(0);
      setSelectionEnd(0);
      setPlayheadSeconds(0);
      setEditorNotice("Reverted to the previous backend edit state.");
    } catch (error) {
      setEditorNotice(error instanceof Error ? error.message : "Nothing earlier to undo.");
    }
  }

  async function handleRedo() {
    if (!activeClip) {
      return;
    }

    pausePlayback();
    try {
      const updated = await onRedo(activeClip.id);
      setDraftTranscript(getSliceTranscriptText(updated));
      setDraftTags(updated.tags.map((tag) => tag.name).join(", "));
      setSelectionStart(0);
      setSelectionEnd(0);
      setPlayheadSeconds(0);
      setEditorNotice("Re-applied the next backend edit state.");
    } catch (error) {
      setEditorNotice(error instanceof Error ? error.message : "Nothing newer to redo.");
    }
  }

  async function handleSplitClip() {
    if (!activeClip) {
      return;
    }

    const splitAt =
      normalizedSelectionEnd > 0 && normalizedSelectionEnd < activeDuration
        ? normalizedSelectionEnd
        : Number((activeDuration / 2).toFixed(2));

    if (splitAt <= 0 || splitAt >= activeDuration) {
      setEditorNotice("Choose a valid split point inside the slice.");
      return;
    }

    setIsApplyingEdit(true);
    pausePlayback();
    try {
      const result = await onSplitClip(activeClip.id, splitAt);
      setEditorNotice(`Split ${activeClip.id} into ${result.length} visible slices.`);
    } catch (error) {
      setEditorNotice(error instanceof Error ? error.message : "Split failed.");
    } finally {
      setIsApplyingEdit(false);
    }
  }

  async function handleMergeClip() {
    if (!activeClip) {
      return;
    }

    setIsApplyingEdit(true);
    pausePlayback();
    try {
      const result = await onMergeClip(activeClip.id);
      setEditorNotice(`Merged. Workspace now has ${result.length} visible slices.`);
    } catch (error) {
      setEditorNotice(error instanceof Error ? error.message : "Merge failed.");
    } finally {
      setIsApplyingEdit(false);
    }
  }

  async function handleRunDeepFilterNet() {
    if (!activeClip) {
      return;
    }
    setIsRunningModel(true);
    pausePlayback();
    try {
      const updated = await onRunClipLabModel(activeClip.id, "deepfilternet");
      setEditorNotice(`Activated variant ${updated.active_variant_id ?? "unknown"}.`);
    } catch (error) {
      setEditorNotice(error instanceof Error ? error.message : "Clip Lab model failed.");
    } finally {
      setIsRunningModel(false);
    }
  }

  return (
    <section className="editor-column">
      <section className="panel waveform-panel">
        <div className="panel-header">
          <div className="clip-editor-header-main">
            <p className="eyebrow">Slice Editor</p>
            {activeClip ? (
              <div className="tag-list header-tag-list">
                {activeClip.tags.length > 0 ? (
                  activeClip.tags.map((tag) => (
                    <span key={`${activeClip.id}-top-${tag.id}`} className="tag-pill" style={{ backgroundColor: tag.color }}>
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
                <span>{formatSeconds(activeDuration)}</span>
                <span>{(activeClip.active_variant?.sample_rate ?? activeClip.source_recording.sample_rate) / 1000} kHz</span>
                <span>{activeClip.source_recording.num_channels} ch</span>
              </div>
              <div className="editor-actions">
                <button
                  className="primary-button"
                  type="button"
                  onClick={() => void handleSaveSlice()}
                  disabled={!activeClip || isSavingSlice}
                >
                  Save Slice
                </button>
                <button
                  className="primary-button"
                  type="button"
                  onClick={() => void handleAcceptNextAndPlay()}
                  disabled={!activeClip || isSavingSlice}
                >
                  Accept & Next
                </button>
                <button
                  className="primary-button"
                  type="button"
                  onClick={() => void handleRejectNextAndPlay()}
                  disabled={!activeClip || isSavingSlice || isApplyingEdit}
                >
                  Reject & Next
                </button>
                <button
                  type="button"
                  onClick={() => void handleRunDeepFilterNet()}
                  disabled={!activeClip || isRunningModel}
                >
                  {isRunningModel ? "Running..." : "Run DeepFilterNet"}
                </button>
              </div>
            </div>
          ) : null}
        </div>

        {editorNotice ? <p className="editor-notice">{editorNotice}</p> : null}

        {workspacePhase === "loading" ? (
          <div className="empty-state">Loading waveform and transcript...</div>
        ) : workspacePhase === "error" ? (
          <WorkspaceStatePanel
            title="Project failed to load"
            message={workspaceError ?? "The editor could not load this project."}
            actionLabel="Retry load"
            onAction={onRetryLoad}
          />
        ) : activeClip ? (
          waveformError ? (
            <div className="waveform-offline-panel" role="alert">
              <strong>Media offline</strong>
              <p>{waveformError}</p>
              <span>
                This slice can still be reviewed for transcript and metadata, but waveform-driven
                editing is unavailable until the source audio is re-linked.
              </span>
            </div>
          ) : (
            <>
              <WaveformPane
                audioUrl={activeClipAudioUrl ?? ""}
                durationSeconds={activeDuration}
                peaks={waveformPeaks?.peaks ?? null}
                desiredCursorSeconds={playheadSeconds}
                selectionStart={selectionStart}
                selectionEnd={selectionEnd}
                onSelectionChange={(start, end) => {
                  setSelectionStart(start);
                  setSelectionEnd(end);
                }}
                onCursorChange={setPlayheadSeconds}
                onHoverTimeChange={setHoverSeconds}
                onReady={(instance) => {
                  waveSurferRef.current = instance;
                  applyPlaybackRate(instance, playbackRate);
                }}
                onPlayingChange={setIsPlaying}
              />

              <div className="waveform-second-scale" aria-hidden="true">
                {Array.from({ length: Math.floor(activeDuration) + 1 }, (_, second) => (
                  <span
                    key={`sec-tick-${activeClip.id}-${second}`}
                    className="waveform-second-tick"
                    style={{ left: `${(second / Math.max(activeDuration, 0.001)) * 100}%` }}
                  >
                    {second}s
                  </span>
                ))}
              </div>

              <div className="timeline-strip transport-strip">
                <span className="transport-pill">
                  {formatClipTimestamp(playheadSeconds)} / {formatClipTimestamp(activeDuration)}
                </span>
                <span className="transport-meta">
                  {hoverSeconds !== null ? `Hover ${formatClipTimestamp(hoverSeconds)}` : "Hover --.--"}
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
                <button type="button" onClick={() => void handleUndo()} disabled={!canUndo}>
                  Undo
                </button>
                <button type="button" onClick={() => void handleRedo()} disabled={!canRedo}>
                  Redo
                </button>
                <button type="button" onClick={() => void handleSplitClip()} disabled={isApplyingEdit}>
                  {isApplyingEdit ? "Applying..." : "Split Slice"}
                </button>
                <button type="button" onClick={() => void handleMergeClip()} disabled={isApplyingEdit}>
                  Merge Next Slice
                </button>
                {hasActiveSelection ? (
                  <button type="button" onClick={() => void handleDeleteSelection()} disabled={isApplyingEdit}>
                    Delete Selection
                  </button>
                ) : null}
                <button type="button" onClick={() => void handleInsertSilence()} disabled={isApplyingEdit}>
                  Insert Silence
                </button>
              </div>
            </>
          )
        ) : (
          <div className="empty-state">
            {workspacePhase === "empty"
              ? workspaceEmptyMessage ?? "Import a project to begin review."
              : "Select a slice to begin review."}
          </div>
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
          onChange={(event) => setDraftTranscript(event.target.value)}
          placeholder="Transcript text"
        />

        <div className="selection-panel">
          <div className="selection-header">
            <strong>Tags</strong>
            <button className="primary-button" type="button" onClick={() => void handleSaveSlice()}>
              Save Slice
            </button>
          </div>
          <p className="muted-copy">
            Pipeline status is strict control flow. Tags are subjective QA metadata.
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
              <span className="muted-copy">No tags on this slice yet.</span>
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
            Source: <strong>{activeClip ? getAlignmentSource(activeClip) : "n/a"}</strong>
          </span>
          <span>
            Confidence:{" "}
            <strong>
              {activeClip && getAlignmentConfidence(activeClip) !== null
                ? `${Math.round((getAlignmentConfidence(activeClip) ?? 0) * 100)}%`
                : "n/a"}
            </strong>
          </span>
          <span>
            Redo: <strong>{activeClip && getRedoTarget(activeClip) ? "available" : "none"}</strong>
          </span>
        </div>
      </section>
    </section>
  );
}
