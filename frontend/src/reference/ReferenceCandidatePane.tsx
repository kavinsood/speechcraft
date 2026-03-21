import { buildReferenceCandidateAudioUrl } from "../api";
import type { ReferenceAssetSummary, ReferenceCandidate, ReferenceRun } from "../types";
import { formatReferenceDuration } from "./reference-helpers";

type ReferenceCandidatePaneProps = {
  selectedRun: ReferenceRun | null;
  candidates: ReferenceCandidate[];
  candidateError: string | null;
  promotingCandidateId: string | null;
  promotedCandidatesById: Map<string, ReferenceAssetSummary>;
  onPromoteCandidate: (candidate: ReferenceCandidate) => void;
  onOpenExistingAsset: (assetId: string) => void;
};

export default function ReferenceCandidatePane({
  selectedRun,
  candidates,
  candidateError,
  promotingCandidateId,
  promotedCandidatesById,
  onPromoteCandidate,
  onOpenExistingAsset,
}: ReferenceCandidatePaneProps) {
  return (
    <main className="stage-main">
      <section className="panel stage-placeholder-hero">
        <p className="eyebrow">Reference workstation</p>
        <h3>Run, browse, preview, promote.</h3>
        <p>
          This slice proves the real picker spine: source-backed runs, deterministic candidates,
          lazy preview audio, and promote-as-is into the shared reference library.
        </p>
      </section>

      <section className="panel transcript-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Candidate output</p>
            <h3>{selectedRun ? `Run ${selectedRun.id}` : "No run selected"}</h3>
          </div>
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

        {selectedRun?.status === "completed" && candidates.length === 0 ? (
          <div className="empty-state">This run completed without any usable candidates.</div>
        ) : null}

        {selectedRun?.status === "completed" && candidates.length > 0 ? (
          <div className="reference-candidate-list">
            {candidates.map((candidate) => {
              const audioUrl = buildReferenceCandidateAudioUrl(candidate.run_id, candidate.candidate_id);
              const overallScore = candidate.default_scores.both ?? candidate.default_scores.overall ?? 0;
              const existingAsset = promotedCandidatesById.get(candidate.candidate_id) ?? null;
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
                  {existingAsset ? (
                    <div className="reference-promoted-banner">
                      Already saved as <strong>{existingAsset.name}</strong>.
                    </div>
                  ) : null}
                  <audio controls preload="none" src={audioUrl}>
                    <track kind="captions" />
                  </audio>
                  <div className="button-row">
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
