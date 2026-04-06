import type { ReferenceRun, SourceRecording } from "../types";
import { formatReferenceDuration } from "./reference-helpers";

type ReferenceRunSidebarProps = {
  pageStatus: "loading" | "ready" | "error";
  pageError: string | null;
  sourceRecordings: SourceRecording[];
  selectedRecordingIds: string[];
  referenceRuns: ReferenceRun[];
  selectedRunId: string | null;
  isCreatingRun: boolean;
  onRetryLoad: () => void;
  onToggleRecording: (recordingId: string) => void;
  onCreateRun: () => void;
  onSelectRun: (runId: string) => void;
};

export default function ReferenceRunSidebar({
  pageStatus,
  pageError,
  sourceRecordings,
  selectedRecordingIds,
  referenceRuns,
  selectedRunId,
  isCreatingRun,
  onRetryLoad,
  onToggleRecording,
  onCreateRun,
  onSelectRun,
}: ReferenceRunSidebarProps) {
  return (
    <aside className="stage-sidebar panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Picker runs</p>
          <h3>Candidate input</h3>
        </div>
      </div>

      {pageStatus === "loading" ? <div className="empty-state">Loading source recordings...</div> : null}
      {pageStatus === "error" ? (
        <div className="empty-state">
          <p>{pageError ?? "The source recording list could not be loaded."}</p>
          <div className="button-row">
            <button className="primary-button" type="button" onClick={onRetryLoad}>
              Retry load
            </button>
          </div>
        </div>
      ) : null}
      {pageStatus === "ready" && sourceRecordings.length === 0 ? (
        <div className="empty-state">This project does not have any source recordings yet.</div>
      ) : null}
      {pageStatus === "ready" && sourceRecordings.length > 0 ? (
        <>
          <div className="selection-panel reference-source-panel">
            <p className="muted-copy">
              Runs are temporary discovery output. Promoted references become durable library assets.
            </p>
            <div className="reference-checkbox-list">
              {sourceRecordings.map((recording) => (
                <label key={recording.id} className="reference-checkbox-row">
                  <input
                    type="checkbox"
                    checked={selectedRecordingIds.includes(recording.id)}
                    onChange={() => onToggleRecording(recording.id)}
                  />
                  <span>
                    <strong>{recording.id}</strong>
                    <small>
                      {formatReferenceDuration(recording.duration_seconds)} • {recording.sample_rate / 1000} kHz
                      {recording.processing_recipe ? ` • ${recording.processing_recipe}` : " • original"}
                    </small>
                  </span>
                </label>
              ))}
            </div>
            <button
              className="primary-button"
              type="button"
              disabled={isCreatingRun || selectedRecordingIds.length === 0}
              onClick={onCreateRun}
            >
              {isCreatingRun ? "Starting run..." : "Start Candidate Run"}
            </button>
          </div>

          <div className="panel-header compact-header">
            <div>
              <p className="eyebrow">Run history</p>
              <h3>Recent runs</h3>
            </div>
          </div>
          {referenceRuns.length === 0 ? (
            <div className="empty-state">No candidate runs yet.</div>
          ) : (
            <div className="commit-list">
              {referenceRuns.map((run) => (
                <button
                  key={run.id}
                  type="button"
                  className={`commit-card ${run.id === selectedRunId ? "selected" : ""}`}
                  onClick={() => onSelectRun(run.id)}
                >
                  <div className="commit-row">
                    <strong>{run.id}</strong>
                    <span>{run.status}</span>
                  </div>
                  <p>
                    {(Array.isArray(run.config?.recording_ids) ? run.config?.recording_ids.length : 0) || 0} recording(s)
                    {" • "}
                    {run.candidate_count} candidate(s)
                  </p>
                  <span className="commit-time">
                    {run.mode}
                    {run.completed_at ? ` • ${new Date(run.completed_at).toLocaleString()}` : ""}
                  </span>
                </button>
              ))}
            </div>
          )}
        </>
      ) : null}
    </aside>
  );
}
