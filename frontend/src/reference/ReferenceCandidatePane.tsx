import { buildReferenceCandidateAudioUrl } from "../api";
import type { ReferenceAssetSummary, ReferenceCandidate, ReferenceRerankCandidate, ReferenceRun } from "../types";
import { formatReferenceDuration } from "./reference-helpers";

type CandidateListEntry = ReferenceCandidate | ReferenceRerankCandidate;

type ReferenceCandidatePaneProps = {
  selectedRun: ReferenceRun | null;
  candidates: CandidateListEntry[];
  candidateError: string | null;
  isReranking: boolean;
  positiveCandidateIds: string[];
  negativeCandidateIds: string[];
  promotingCandidateId: string | null;
  promotedCandidatesById: Map<string, ReferenceAssetSummary>;
  onTogglePositiveCandidate: (candidateId: string) => void;
  onToggleNegativeCandidate: (candidateId: string) => void;
  onResetRerankAnchors: () => void;
  onPromoteCandidate: (candidate: ReferenceCandidate) => void;
  onOpenExistingAsset: (assetId: string) => void;
};

export default function ReferenceCandidatePane({
  selectedRun,
  candidates,
  candidateError,
  isReranking,
  positiveCandidateIds,
  negativeCandidateIds,
  promotingCandidateId,
  promotedCandidatesById,
  onTogglePositiveCandidate,
  onToggleNegativeCandidate,
  onResetRerankAnchors,
  onPromoteCandidate,
  onOpenExistingAsset,
}: ReferenceCandidatePaneProps) {
  return (
    <main className="stage-main">
      <section className="panel stage-placeholder-hero">
        <p className="eyebrow">Reference workstation</p>
        <h3>Run, browse, rerank, preview, promote.</h3>
        <p>
          This slice proves the real picker spine: source-backed runs, deterministic candidates,
          intent shaping with likes and dislikes, lazy preview audio, and promotion into the shared reference library.
        </p>
      </section>

      <section className="panel transcript-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Candidate output</p>
            <h3>{selectedRun ? `Run ${selectedRun.id}` : "No run selected"}</h3>
          </div>
          {selectedRun?.status === "completed" ? (
            <div className="reference-rerank-summary">
              <span>
                Likes: <strong>{positiveCandidateIds.length}</strong>
              </span>
              <span>
                Dislikes: <strong>{negativeCandidateIds.length}</strong>
              </span>
              <button
                type="button"
                disabled={positiveCandidateIds.length === 0 && negativeCandidateIds.length === 0}
                onClick={onResetRerankAnchors}
              >
                Reset rerank
              </button>
            </div>
          ) : null}
        </div>

        {!selectedRun ? (
          <div className="empty-state">
            Start a candidate run from one or more source recordings to populate this pane.
          </div>
        ) : null}

        {selectedRun && selectedRun.status !== "completed" ? (
          <div className="commit-card selected">
            <div className="commit-row">
              <strong>{selectedRun.id}</strong>
              <span>{selectedRun.status}</span>
            </div>
            <p>
              {selectedRun.status === "failed"
                ? selectedRun.error_message || "The run failed."
                : "The backend is materializing candidate artifacts. This page will refresh automatically."}
            </p>
          </div>
        ) : null}

        {candidateError ? <div className="empty-state">{candidateError}</div> : null}
        {selectedRun?.status === "completed" && isReranking ? (
          <div className="empty-state">Reranking candidates with the current likes and dislikes...</div>
        ) : null}

        {selectedRun?.status === "completed" && candidates.length === 0 ? (
          <div className="empty-state">This run completed without any usable candidates.</div>
        ) : null}

        {selectedRun?.status === "completed" && candidates.length > 0 ? (
          <div className="reference-candidate-list">
            {candidates.map((candidate) => {
              const audioUrl = buildReferenceCandidateAudioUrl(candidate.run_id, candidate.candidate_id);
              const isReranked = "rerank_score" in candidate;
              const overallScore = isReranked
                ? candidate.rerank_score
                : (candidate.default_scores.both ?? candidate.default_scores.overall ?? 0);
              const existingAsset = promotedCandidatesById.get(candidate.candidate_id) ?? null;
              const isPositive = positiveCandidateIds.includes(candidate.candidate_id);
              const isNegative = negativeCandidateIds.includes(candidate.candidate_id);
              return (
                <article key={candidate.candidate_id} className="reference-candidate-card">
                  <div className="commit-row">
                    <strong>{candidate.transcript_text || candidate.candidate_id}</strong>
                    <span>{overallScore.toFixed(3)}</span>
                  </div>
                  <p>
                    {candidate.speaker_name || "speaker n/a"}
                    {candidate.language ? ` • ${candidate.language}` : ""}
                    {candidate.source_recording_id ? ` • ${candidate.source_recording_id}` : ""}
                  </p>
                  <span className="commit-time">
                    {formatReferenceDuration(candidate.duration_seconds)} • {candidate.source_start_seconds.toFixed(2)}s to{" "}
                    {candidate.source_end_seconds.toFixed(2)}s
                  </span>
                  {candidate.risk_flags.length > 0 ? (
                    <div className="reference-candidate-flags">
                      {candidate.risk_flags.map((flag) => (
                        <span key={flag} className="reference-flag-chip">
                          {flag.split("_").join(" ")}
                        </span>
                      ))}
                    </div>
                  ) : null}
                  {isReranked ? (
                    <div className="reference-rerank-metrics">
                      <span>base {candidate.base_score.toFixed(3)}</span>
                      <span>intent {candidate.intent_score >= 0 ? "+" : ""}{candidate.intent_score.toFixed(3)}</span>
                      <span>mode {candidate.mode}</span>
                    </div>
                  ) : null}
                  {existingAsset ? (
                    <div className="reference-promoted-banner">
                      Already saved as <strong>{existingAsset.name}</strong>.
                    </div>
                  ) : null}
                  <audio controls preload="none" src={audioUrl}>
                    <track kind="captions" />
                  </audio>
                  <div className="button-row">
                    <button
                      type="button"
                      className={isPositive ? "primary-button" : ""}
                      onClick={() => onTogglePositiveCandidate(candidate.candidate_id)}
                    >
                      {isPositive ? "Liked" : "Like"}
                    </button>
                    <button
                      type="button"
                      className={isNegative ? "primary-button" : ""}
                      onClick={() => onToggleNegativeCandidate(candidate.candidate_id)}
                    >
                      {isNegative ? "Disliked" : "Dislike"}
                    </button>
                    {existingAsset ? (
                      <>
                        <button
                          className="primary-button"
                          type="button"
                          onClick={() => onOpenExistingAsset(existingAsset.id)}
                        >
                          Open Existing
                        </button>
                        <button
                          type="button"
                          disabled={promotingCandidateId === candidate.candidate_id}
                          onClick={() => onPromoteCandidate(candidate)}
                        >
                          {promotingCandidateId === candidate.candidate_id ? "Promoting..." : "Promote Again"}
                        </button>
                      </>
                    ) : (
                      <button
                        className="primary-button"
                        type="button"
                        disabled={promotingCandidateId === candidate.candidate_id}
                        onClick={() => onPromoteCandidate(candidate)}
                      >
                        {promotingCandidateId === candidate.candidate_id ? "Promoting..." : "Promote As Reference"}
                      </button>
                    )}
                  </div>
                </article>
              );
            })}
          </div>
        ) : null}
      </section>
    </main>
  );
}
