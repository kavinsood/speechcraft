import type { ExportRun, ReviewStatus, Slice } from "../types";
import WorkspaceStatePanel from "./WorkspaceStatePanel";
import {
  formatDurationCompact,
  formatSeconds,
  getSliceLanguage,
  getSliceOriginalEnd,
  getSliceOriginalStart,
  getSliceSpeakerName,
  getSliceTranscriptText,
  queuePriorityOrder,
  sortVariantsForHistory,
  statusLabels,
} from "./workspace-helpers";

type WorkspacePhase = "loading" | "error" | "empty" | "ready";

type StatusCountMap = Record<ReviewStatus, number>;
type StatusDurationMap = Record<ReviewStatus, number>;

type InspectorPaneProps = {
  workspacePhase: WorkspacePhase;
  workspaceError: string | null;
  activeClip: Slice | null;
  totalClipCount: number;
  totalDurationSeconds: number;
  datasetStatusCounts: {
    counts: StatusCountMap;
    durations: StatusDurationMap;
  };
  acceptedRejectedRatio: number | null;
  predictedOutputSeconds: number | null;
  progressPercent: number | null;
  exportRuns: ExportRun[];
  onRetryLoad: () => void;
  onStatusChange: (status: ReviewStatus) => void;
  onVariantSelect: (variantId: string) => void;
};

export default function InspectorPane({
  workspacePhase,
  workspaceError,
  activeClip,
  totalClipCount,
  totalDurationSeconds,
  datasetStatusCounts,
  acceptedRejectedRatio,
  predictedOutputSeconds,
  progressPercent,
  exportRuns,
  onRetryLoad,
  onStatusChange,
  onVariantSelect,
}: InspectorPaneProps) {
  return (
    <aside className="inspector-column panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Inspector</p>
          <h2>Slice Review</h2>
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
          <section className="inspector-block">
            <h3>Pipeline Status</h3>
            <div className="status-group">
              {queuePriorityOrder.map((status) => (
                <button
                  key={status}
                  className={`status-button ${activeClip.status === status ? "selected" : ""}`}
                  type="button"
                  onClick={() => onStatusChange(status)}
                >
                  {statusLabels[status]}
                </button>
              ))}
            </div>
          </section>

          <section className="inspector-block">
            <h3>User Tags</h3>
            <div className="tag-list">
              {activeClip.tags.length > 0 ? (
                activeClip.tags.map((tag) => (
                  <span key={tag.id} className="tag-pill" style={{ backgroundColor: tag.color }}>
                    {tag.name}
                  </span>
                ))
              ) : (
                <p className="muted-copy">No tags on this slice yet.</p>
              )}
            </div>
          </section>

          <section className="inspector-block">
            <div className="stats-table">
              <div className="stats-row stats-head">
                <span>Status</span>
                <span>Slices</span>
                <span>Length</span>
              </div>
              <div className="stats-row">
                <span>Total</span>
                <span>{totalClipCount}</span>
                <span>{formatDurationCompact(totalDurationSeconds)}</span>
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
                <span>A/R Ratio (Duration)</span>
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
            <h3>Waveform EDL</h3>
            {activeClip.active_commit?.edl_operations.length ? (
              <ul className="edl-list">
                {activeClip.active_commit.edl_operations.map((operation, index) => (
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
              <p className="muted-copy">No waveform math stored yet.</p>
            )}
          </section>

          <section className="inspector-block">
            <h3>Slice History</h3>
            {activeClip.commits.length > 0 ? (
              <div className="commit-list">
                {[...activeClip.commits].reverse().map((commitEntry) => (
                  <div key={commitEntry.id} className="commit-card">
                    <div className="commit-row">
                      <strong>{commitEntry.message ?? "Auto revision"}</strong>
                      <span>
                        {commitEntry.is_milestone ? "milestone" : "auto"} • {statusLabels[commitEntry.status]}
                      </span>
                    </div>
                    <p>{commitEntry.transcript_text || "(blank transcript)"}</p>
                    <span className="commit-time">
                      {new Date(commitEntry.created_at).toLocaleString()}
                    </span>
                  </div>
                ))}
              </div>
            ) : (
              <p className="muted-copy">No saved slice history yet.</p>
            )}
          </section>

          <section className="inspector-block">
            <h3>Clip Lab History</h3>
            {activeClip.variants.length > 0 ? (
              <div className="commit-list">
                {sortVariantsForHistory(activeClip.variants).map((variant) => (
                  <button
                    key={variant.id}
                    type="button"
                    className={`commit-card ${variant.id === activeClip.active_variant_id ? "selected" : ""}`}
                    onClick={() => onVariantSelect(variant.id)}
                  >
                    <div className="commit-row">
                      <strong>{variant.generator_model ?? "variant"}</strong>
                      <span>{variant.id === activeClip.active_variant_id ? "active" : "available"}</span>
                    </div>
                    <p>{variant.is_original ? "Original slicer output" : "Derived variant"}</p>
                    <span className="commit-time">
                      {variant.sample_rate / 1000} kHz • {Math.round(variant.num_samples / Math.max(variant.sample_rate, 1) * 100) / 100}s
                    </span>
                  </button>
                ))}
              </div>
            ) : (
              <p className="muted-copy">No variants attached to this slice.</p>
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
                      {run.accepted_clip_count} slice(s)
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
                <dd>{activeClip.source_recording.id}</dd>
              </div>
              <div>
                <dt>Speaker</dt>
                <dd>{getSliceSpeakerName(activeClip)}</dd>
              </div>
              <div>
                <dt>Language</dt>
                <dd>{getSliceLanguage(activeClip)}</dd>
              </div>
              <div>
                <dt>Original Range</dt>
                <dd>
                  {formatSeconds(getSliceOriginalStart(activeClip))} to{" "}
                  {formatSeconds(getSliceOriginalEnd(activeClip))}
                </dd>
              </div>
              <div>
                <dt>Transcript</dt>
                <dd>{getSliceTranscriptText(activeClip) || "n/a"}</dd>
              </div>
            </dl>
          </section>
        </>
      ) : (
        <div className="empty-state">
          {workspacePhase === "empty" ? "No project selected." : "No slice selected."}
        </div>
      )}
    </aside>
  );
}
