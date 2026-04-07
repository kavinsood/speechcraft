import { useEffect, useMemo, useRef, useState } from "react";
import type WaveSurfer from "wavesurfer.js";
import WaveformPane from "../WaveformPane";
import { fetchClipLabWaveformPeaks } from "../api";
import type { ClipLabItem, ClipLabItemRef, ReviewStatus, WaveformPeaks } from "../types";
import WorkspaceStatePanel from "./WorkspaceStatePanel";
import {
  formatClipTimestamp,
  formatSeconds,
  getAlignmentSource,
  getSliceDuration,
  getSliceTranscriptText,
  parseTagDraft,
} from "./workspace-helpers";

type WorkspacePhase = "loading" | "error" | "empty" | "ready";

type EditorPaneProps = {
  workspacePhase: WorkspacePhase;
  workspaceError: string | null;
  workspaceEmptyMessage: string | null;
  activeClip: ClipLabItem | null;
  activeClipAudioUrl: string | null;
  canUndo: boolean;
  canRedo: boolean;
  allClipTagNames: string[];
  getNextClipItem: (currentClipItem: ClipLabItemRef) => ClipLabItemRef | null;
  onSelectClip: (clipItem: ClipLabItemRef) => void;
  onRetryLoad: () => void;
  onSaveClipLabItem: (
    clipItem: ClipLabItemRef,
    payload: {
      modified_text?: string | null;
      tags?: { name: string; color: string }[] | null;
      status?: ReviewStatus | null;
      message?: string | null;
      is_milestone?: boolean;
    },
  ) => Promise<ClipLabItem>;
  onAppendEdlOperation: (
    clipItem: ClipLabItemRef,
    payload: {
      op: string;
      range?: { start_seconds: number; end_seconds: number } | null;
      duration_seconds?: number | null;
    },
  ) => Promise<ClipLabItem>;
  onUndo: (clipItem: ClipLabItemRef) => Promise<ClipLabItem>;
  onRedo: (clipItem: ClipLabItemRef) => Promise<ClipLabItem>;
  onSplitClip: (clipItem: ClipLabItemRef, splitAtSeconds: number) => Promise<number>;
  onMergeClip: (clipItem: ClipLabItemRef) => Promise<number>;
  onRunClipLabModel: (clipItem: ClipLabItemRef, generatorModel: string) => Promise<ClipLabItem>;
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
  getNextClipItem,
  onSelectClip,
  onRetryLoad,
  onSaveClipLabItem,
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
  const activeAudioRevisionKey = activeClip
    ? JSON.stringify({
        audio_url: activeClip.audio_url,
        active_variant_id: activeClip.active_variant?.id ?? null,
        active_commit_id: activeClip.active_commit?.id ?? null,
        edl_operations: activeClip.active_commit?.edl_operations ?? [],
      })
    : null;
  const capabilities = activeClip?.capabilities;
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
        const nextPeaks = await fetchClipLabWaveformPeaks(activeClip.id, 960);
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
  }, [activeClip?.id, activeAudioRevisionKey]);

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

  async function handleSaveClip(forceStatus?: ReviewStatus): Promise<ClipLabItem | null> {
    if (!activeClip || !capabilities?.can_save) {
      return null;
    }

    setIsSavingSlice(true);
    pausePlayback();

    try {
      const nextTags = parseTagDraft(draftTags);
      const currentTagDraft = activeClip.tags.map((tag) => tag.name).join(", ");
      const workingClip = await onSaveClipLabItem({ id: activeClip.id }, {
        modified_text:
          draftTranscript !== getSliceTranscriptText(activeClip) ? draftTranscript : undefined,
        tags: draftTags.trim() !== currentTagDraft.trim() ? nextTags : undefined,
        status: forceStatus && activeClip.status !== forceStatus ? forceStatus : undefined,
        message:
          forceStatus === "accepted"
            ? "Accepted clip milestone"
            : forceStatus === "rejected"
              ? "Rejected clip milestone"
              : "Saved clip milestone",
        is_milestone: true,
      });
      setEditorNotice("Saved clip state.");
      setDraftTranscript(getSliceTranscriptText(workingClip));
      setDraftTags(workingClip.tags.map((tag) => tag.name).join(", "));
      return workingClip;
    } catch (error) {
      setEditorNotice(error instanceof Error ? error.message : "Save failed.");
      return null;
    } finally {
      setIsSavingSlice(false);
    }
  }

  async function handleAcceptNextAndPlay() {
    if (!activeClip || !capabilities?.can_set_status || isSavingSlice || isApplyingEdit) {
      return;
    }

    const nextClipItem = getNextClipItem({ id: activeClip.id });
    const savedClip = await handleSaveClip("accepted");
    if (!savedClip) {
      return;
    }

    if (!nextClipItem) {
      setEditorNotice("Saved. No next item in the current queue.");
      return;
    }

    shouldAutoPlayAfterClipChangeRef.current = true;
    onSelectClip(nextClipItem);
  }

  async function handleRejectNextAndPlay() {
    if (!activeClip || !capabilities?.can_set_status || isSavingSlice || isApplyingEdit) {
      return;
    }

    const nextClipItem = getNextClipItem({ id: activeClip.id });
    const savedClip = await handleSaveClip("rejected");
    if (!savedClip) {
      return;
    }

    if (!nextClipItem) {
      return;
    }

    shouldAutoPlayAfterClipChangeRef.current = true;
    onSelectClip(nextClipItem);
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
    if (!activeClip || !capabilities?.can_edit_waveform) {
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
      const updated = await onAppendEdlOperation({ id: activeClip.id }, {
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
    if (!activeClip || !capabilities?.can_edit_waveform) {
      return;
    }

    const cursorTime = waveSurferRef.current
      ? Number(waveSurferRef.current.getCurrentTime().toFixed(2))
      : playheadSeconds;
    const insertAt = Math.max(0, Math.min(cursorTime, activeDuration));

    setIsApplyingEdit(true);
    pausePlayback();
    try {
      const updated = await onAppendEdlOperation({ id: activeClip.id }, {
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
    if (!activeClip || !canUndo) {
      return;
    }

    pausePlayback();
    try {
      const updated = await onUndo({ id: activeClip.id });
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
    if (!activeClip || !canRedo) {
      return;
    }

    pausePlayback();
    try {
      const updated = await onRedo({ id: activeClip.id });
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
    if (!activeClip || !capabilities?.can_split) {
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
      const count = await onSplitClip({ id: activeClip.id }, splitAt);
      setEditorNotice(`Split ${activeClip.id}. Workspace now shows ${count} visible item(s).`);
    } catch (error) {
      setEditorNotice(error instanceof Error ? error.message : "Split failed.");
    } finally {
      setIsApplyingEdit(false);
    }
  }

  async function handleMergeClip() {
    if (!activeClip || !capabilities?.can_merge) {
      return;
    }

    setIsApplyingEdit(true);
    pausePlayback();
    try {
      const count = await onMergeClip({ id: activeClip.id });
      setEditorNotice(`Merged. Workspace now has ${count} visible item(s).`);
    } catch (error) {
      setEditorNotice(error instanceof Error ? error.message : "Merge failed.");
    } finally {
      setIsApplyingEdit(false);
    }
  }

  async function handleRunDeepFilterNet() {
    if (!activeClip || !capabilities?.can_run_processing) {
      return;
    }
    setIsRunningModel(true);
    pausePlayback();
    try {
      const updated = await onRunClipLabModel({ id: activeClip.id }, "deepfilternet");
      setEditorNotice(`Activated variant ${updated.active_variant?.id ?? "unknown"}.`);
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
            <p className="eyebrow">Clip Lab</p>
            {activeClip ? (
              <div className="tag-list header-tag-list">
                {activeClip.tags.length > 0 ? (
                  activeClip.tags.map((tag) => (
                    <span key={`${activeClip.id}-top-${tag.id}`} className="tag-pill" style={{ backgroundColor: tag.color }}>
                      {tag.name}
                    </span>
                  ))
                ) : null}
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
                  onClick={() => void handleSaveClip()}
                  disabled={!activeClip || !capabilities?.can_save || isSavingSlice}
                >
                  Save
                </button>
                <button
                  className="primary-button"
                  type="button"
                  onClick={() => void handleAcceptNextAndPlay()}
                  disabled={!activeClip || !capabilities?.can_set_status || isSavingSlice}
                >
                  Accept & Next
                </button>
                <button
                  className="primary-button"
                  type="button"
                  onClick={() => void handleRejectNextAndPlay()}
                  disabled={!activeClip || !capabilities?.can_set_status || isSavingSlice || isApplyingEdit}
                >
                  Reject & Next
                </button>
                <button
                  type="button"
                  onClick={() => void handleRunDeepFilterNet()}
                  disabled={!activeClip || !capabilities?.can_run_processing || isRunningModel}
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
                <button
                  type="button"
                  onClick={() => void handleSplitClip()}
                  disabled={!capabilities?.can_split || isApplyingEdit}
                >
                  {isApplyingEdit ? "Applying..." : "Split Item"}
                </button>
                <button
                  type="button"
                  onClick={() => void handleMergeClip()}
                  disabled={!capabilities?.can_merge || isApplyingEdit}
                >
                  Merge Next Item
                </button>
                {hasActiveSelection ? (
                  <button
                    type="button"
                    onClick={() => void handleDeleteSelection()}
                    disabled={!capabilities?.can_edit_waveform || isApplyingEdit}
                  >
                    Delete Selection
                  </button>
                ) : null}
                <button
                  type="button"
                  onClick={() => void handleInsertSilence()}
                  disabled={!capabilities?.can_edit_waveform || isApplyingEdit}
                >
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
          disabled={!capabilities?.can_edit_transcript}
        />

        <div className="transcript-footer">
          <span>
            Source: <strong>{activeClip ? getAlignmentSource(activeClip) : "n/a"}</strong>
          </span>
          <span>
            Redo: <strong>{activeClip && canRedo ? "available" : "none"}</strong>
          </span>
        </div>

        <div className="transcript-meta-grid">
          {activeClip ? (
            <div className="selection-panel asr-panel">
              <div className="selection-header">
                <strong>ASR</strong>
                <button type="button" disabled={!activeClip.can_run_asr}>
                  {activeClip.can_run_asr ? "Run ASR" : "Run ASR (Soon)"}
                </button>
              </div>
              <p className="muted-copy">
                Source: {activeClip.transcript_source ?? "unknown"}
              </p>
              <p className="muted-copy">
                {activeClip.asr_placeholder_message ?? "ASR controls will land here."}
              </p>
            </div>
          ) : null}

          <div className="selection-panel">
            <div className="selection-header">
              <strong>Tags</strong>
              <button
                className="primary-button"
                type="button"
                onClick={() => void handleSaveClip()}
                disabled={!capabilities?.can_save}
              >
                Save
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
                    disabled={!capabilities?.can_edit_tags}
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
                  if (!capabilities?.can_edit_tags) {
                    return;
                  }
                  if (event.key === "Enter" || event.key === ",") {
                    event.preventDefault();
                    handleAddTagFromInput();
                  }
                }}
                placeholder="Add tag (press Enter)"
                disabled={!capabilities?.can_edit_tags}
              />
              <button type="button" onClick={handleAddTagFromInput} disabled={!capabilities?.can_edit_tags}>
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
                  disabled={!capabilities?.can_edit_tags}
                >
                  + {tagName}
                </button>
              ))}
            </div>
          </div>
        </div>
      </section>
    </section>
  );
}
