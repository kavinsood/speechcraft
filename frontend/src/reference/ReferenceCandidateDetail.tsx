import { useEffect, useMemo, useState } from "react";

import { buildReferenceCandidateAudioUrl } from "../api";
import type { ReferenceAssetSummary, ReferenceCandidate, ReferenceRerankCandidate } from "../types";
import { formatReferenceDuration } from "./reference-helpers";
import {
  clampTrimOffsets,
  computeReferenceTrimSuggestion,
  type ReferenceTrimSuggestion,
  validateManualTrimOffsets,
} from "./reference-trim-helpers";

type CandidateListEntry = ReferenceCandidate | ReferenceRerankCandidate;

type ReferenceCandidateDetailProps = {
  candidate: CandidateListEntry | null;
  existingAssets: ReferenceAssetSummary[];
  isPromoting: boolean;
  onPromoteCandidate: (candidate: ReferenceCandidate, startSeconds: number, endSeconds: number) => void;
  onOpenExistingAsset: (assetId: string) => void;
};

type TrimLoadState = "idle" | "loading" | "ready" | "error";

export default function ReferenceCandidateDetail({
  candidate,
  existingAssets,
  isPromoting,
  onPromoteCandidate,
  onOpenExistingAsset,
}: ReferenceCandidateDetailProps) {
  const [trimState, setTrimState] = useState<ReferenceTrimSuggestion | null>(null);
  const [suggestedTrim, setSuggestedTrim] = useState<ReferenceTrimSuggestion | null>(null);
  const [trimLoadState, setTrimLoadState] = useState<TrimLoadState>("idle");
  const [trimError, setTrimError] = useState<string | null>(null);
  const [trimStartInput, setTrimStartInput] = useState("0.00");
  const [trimEndInput, setTrimEndInput] = useState("0.00");
  const [trimInputError, setTrimInputError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    if (!candidate) {
      setTrimState(null);
      setSuggestedTrim(null);
      setTrimLoadState("idle");
      setTrimError(null);
      return;
    }

    const fullSpan = clampTrimOffsets(0, candidate.duration_seconds, candidate.duration_seconds);
    setTrimState(fullSpan);
    setTrimStartInput(fullSpan.startOffsetSeconds.toFixed(2));
    setTrimEndInput(fullSpan.endOffsetSeconds.toFixed(2));
    setSuggestedTrim(null);
    setTrimLoadState("loading");
    setTrimError(null);
    setTrimInputError(null);

    void (async () => {
      try {
        const suggestion = await computeReferenceTrimSuggestion(
          buildReferenceCandidateAudioUrl(candidate.run_id, candidate.candidate_id),
        );
        if (cancelled) {
          return;
        }
        const normalizedSuggestion = clampTrimOffsets(
          suggestion.startOffsetSeconds,
          suggestion.endOffsetSeconds,
          candidate.duration_seconds,
        );
        setSuggestedTrim(normalizedSuggestion);
        setTrimLoadState("ready");
      } catch (error) {
        if (cancelled) {
          return;
        }
        setTrimState(fullSpan);
        setSuggestedTrim(null);
        setTrimLoadState("error");
        setTrimError(error instanceof Error ? error.message : "Auto-trim could not be computed.");
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [candidate?.candidate_id, candidate?.duration_seconds, candidate?.run_id]);

  const audioUrl = useMemo(() => {
    if (!candidate) {
      return null;
    }
    return buildReferenceCandidateAudioUrl(candidate.run_id, candidate.candidate_id);
  }, [candidate]);

  if (!candidate || !trimState) {
    return (
      <section className="reference-candidate-detail panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Candidate detail</p>
            <h3>No candidate selected</h3>
          </div>
        </div>
        <div className="empty-state">
          Pick a candidate from the list to inspect its preview, apply a suggested trim, and promote a safer subspan.
        </div>
      </section>
    );
  }

  const currentCandidate = candidate;
  const absoluteStartSeconds = currentCandidate.source_start_seconds + trimState.startOffsetSeconds;
  const absoluteEndSeconds = currentCandidate.source_start_seconds + trimState.endOffsetSeconds;
  const latestExistingAsset = existingAssets[0] ?? null;
  const canPromote =
    !isPromoting
    && trimInputError === null
    && Number.isFinite(absoluteStartSeconds)
    && Number.isFinite(absoluteEndSeconds);

  function syncTrimInputs(nextTrim: ReferenceTrimSuggestion) {
    setTrimStartInput(nextTrim.startOffsetSeconds.toFixed(2));
    setTrimEndInput(nextTrim.endOffsetSeconds.toFixed(2));
  }

  function applyManualTrimDrafts(nextStartInput: string, nextEndInput: string) {
    setTrimStartInput(nextStartInput);
    setTrimEndInput(nextEndInput);
    const parsedStart = nextStartInput.trim() === "" ? Number.NaN : Number(nextStartInput);
    const parsedEnd = nextEndInput.trim() === "" ? Number.NaN : Number(nextEndInput);
    const validation = validateManualTrimOffsets(
      parsedStart,
      parsedEnd,
      currentCandidate.duration_seconds,
    );
    if (validation.trim) {
      setTrimState(validation.trim);
      setTrimInputError(null);
    } else {
      setTrimInputError(validation.error);
    }
  }

  function updateTrimStart(nextValue: string) {
    applyManualTrimDrafts(nextValue, trimEndInput);
  }

  function updateTrimEnd(nextValue: string) {
    applyManualTrimDrafts(trimStartInput, nextValue);
  }

  function applySuggestedTrim() {
    if (suggestedTrim) {
      setTrimState(suggestedTrim);
      syncTrimInputs(suggestedTrim);
      setTrimInputError(null);
    }
  }

  function resetTrim() {
    const fullSpan = clampTrimOffsets(0, currentCandidate.duration_seconds, currentCandidate.duration_seconds);
    setTrimState(fullSpan);
    syncTrimInputs(fullSpan);
    setTrimInputError(null);
  }

  return (
    <section className="reference-candidate-detail panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Candidate detail</p>
          <h3>{candidate.transcript_text || candidate.candidate_id}</h3>
        </div>
        <span className="commit-time">
          {formatReferenceDuration(currentCandidate.duration_seconds)} candidate span
        </span>
      </div>

      <p className="reference-candidate-detail-copy">
        Suggested trim is a conservative boundary cleanup over the preview audio. It helps tighten loose edges, but it is not a style-purity detector.
      </p>

      <audio controls preload="metadata" src={audioUrl ?? undefined}>
        <track kind="captions" />
      </audio>

      {existingAssets.length > 0 ? (
        <div className="reference-promoted-banner">
          Saved from this candidate {existingAssets.length} time{existingAssets.length === 1 ? "" : "s"}.
          {latestExistingAsset ? (
            <>
              {" "}
              Latest: <strong>{latestExistingAsset.name}</strong>.
            </>
          ) : null}
        </div>
      ) : null}

      <div className="reference-detail-grid">
        <label className="reference-trim-field">
          <span>Trim start inside candidate</span>
          <input
            type="number"
            step="0.01"
            min="0"
            max={currentCandidate.duration_seconds.toFixed(2)}
            value={trimStartInput}
            onChange={(event) => updateTrimStart(event.target.value)}
          />
        </label>
        <label className="reference-trim-field">
          <span>Trim end inside candidate</span>
          <input
            type="number"
            step="0.01"
            min="0"
            max={currentCandidate.duration_seconds.toFixed(2)}
            value={trimEndInput}
            onChange={(event) => updateTrimEnd(event.target.value)}
          />
        </label>
      </div>

      <div className="reference-trim-summary">
        <span>
          Candidate bounds: {currentCandidate.source_start_seconds.toFixed(2)}s to {currentCandidate.source_end_seconds.toFixed(2)}s
        </span>
        <span>
          Promotion bounds: {absoluteStartSeconds.toFixed(2)}s to {absoluteEndSeconds.toFixed(2)}s
        </span>
      </div>

      <div className="reference-trim-status">
        {trimLoadState === "loading" ? "Computing a suggested trim from the preview audio..." : null}
        {trimLoadState === "ready" && suggestedTrim ? (
          <span>
            Suggested trim keeps {suggestedTrim.startOffsetSeconds.toFixed(2)}s to {suggestedTrim.endOffsetSeconds.toFixed(2)}s inside the candidate.
          </span>
        ) : null}
        {trimInputError ? <span>{trimInputError}</span> : null}
        {trimLoadState === "error" ? (
          <span>{trimError || "Auto-trim suggestion was unavailable. You can still set trim bounds manually."}</span>
        ) : null}
      </div>

      <div className="button-row">
        <button type="button" onClick={resetTrim}>
          Reset to full candidate
        </button>
        <button
          type="button"
          onClick={applySuggestedTrim}
          disabled={!suggestedTrim || trimLoadState !== "ready"}
        >
          Use suggestion
        </button>
        {latestExistingAsset ? (
          <button type="button" onClick={() => onOpenExistingAsset(latestExistingAsset.id)}>
            Open Existing
          </button>
        ) : null}
        <button
          className="primary-button"
          type="button"
          disabled={!canPromote}
          onClick={() => onPromoteCandidate(currentCandidate, absoluteStartSeconds, absoluteEndSeconds)}
        >
          {isPromoting
            ? "Promoting..."
            : existingAssets.length > 0
              ? "Save Another Trim"
              : "Promote Trim"}
        </button>
      </div>
    </section>
  );
}
