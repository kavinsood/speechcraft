import type { Clip, ClipCommit, ExportRun, ProjectDetail, ReviewStatus } from "../types";
import WorkspaceStatePanel from "./WorkspaceStatePanel";
import {
  formatDurationCompact,
  formatSeconds,
  queuePriorityOrder,
  statusLabels,
} from "./workspace-helpers";

type WorkspacePhase = "loading" | "error" | "empty" | "ready";

type StatusCountMap = Record<ReviewStatus, number>;
type StatusDurationMap = Record<ReviewStatus, number>;

type InspectorPaneProps = {
  workspacePhase: WorkspacePhase;
  workspaceError: string | null;
  activeClip: Clip | null;
  projectDetail: ProjectDetail | null;
  datasetStatusCounts: {
    counts: StatusCountMap;
    durations: StatusDurationMap;
  };
  acceptedRejectedRatio: number | null;
  predictedOutputSeconds: number | null;
  progressPercent: number | null;
  activeCommits: ClipCommit[];
  exportRuns: ExportRun[];
  onRetryLoad: () => void;
  onStatusChange: (status: ReviewStatus) => void;
};

export default function InspectorPane({
  workspacePhase,
  workspaceError,
  activeClip,
  projectDetail,
  datasetStatusCounts,
  acceptedRejectedRatio,
  predictedOutputSeconds,
  progressPercent,
  activeCommits,
  exportRuns,
  onRetryLoad,
  onStatusChange,
}: InspectorPaneProps) {
  return (
    <aside className="inspector-column panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Inspector</p>
          <h2>Clip Review</h2>
        </div>
      </div>

      {workspacePhase === "loading" ? (
        <div className="empty-state">Loading project insights...</div>
      ) : workspacePhase === "error" ? (
        <WorkspaceStatePanel
          title="Inspector unavailable"
          message={workspaceError ?? "The inspector could not load this project."}
          actionLabel="Retry load"
          onAction={onRetryLoad}
        />
      ) : activeClip ? (
        <>
          <div className="status-group">
            {queuePriorityOrder.map((status) => (
              <button
                key={status}
                className={`status-button ${activeClip.review_status === status ? "selected" : ""}`}
                type="button"
                onClick={() => onStatusChange(status)}
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
        <div className="empty-state">
          {workspacePhase === "empty" ? "No project selected." : "No clip selected."}
        </div>
      )}
    </aside>
  );
}
