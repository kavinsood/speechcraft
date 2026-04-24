export type JobActivityState = "idle" | "running" | "completed" | "failed";

export type JobActivityLogLine = {
  id: string;
  timestamp: string;
  message: string;
};

export type JobActivity = {
  id: string;
  name: string;
  type: "preparation" | "slicing" | "qc" | "export" | "mock";
  state: JobActivityState;
  startedAt?: string | null;
  completedAt?: string | null;
  progressLabel?: string | null;
  logs: JobActivityLogLine[];
};

type JobActivityPanelProps = {
  title?: string;
  job: JobActivity | null;
  maxLogLines?: number;
};

function formatTime(value?: string | null): string {
  if (!value) {
    return "Not started";
  }

  return new Date(value).toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function getStateLabel(state: JobActivityState): string {
  if (state === "idle") {
    return "Idle";
  }

  if (state === "running") {
    return "Running";
  }

  if (state === "completed") {
    return "Completed";
  }

  return "Failed";
}

export default function JobActivityPanel({
  title = "Job activity",
  job,
  maxLogLines = 300,
}: JobActivityPanelProps) {
  if (!job) {
    return (
      <section className="panel job-activity-panel job-activity-empty">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Activity</p>
            <h3>{title}</h3>
          </div>
          <span className="job-state-pill job-state-idle">Idle</span>
        </div>
        <p className="job-empty-copy">No long-running job is active for this page.</p>
      </section>
    );
  }

  const displayedLogs = job.logs.slice(-maxLogLines);
  const hiddenLogCount = Math.max(0, job.logs.length - displayedLogs.length);

  return (
    <section className="panel job-activity-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">{job.type}</p>
          <h3>{job.name}</h3>
        </div>
        <span className={`job-state-pill job-state-${job.state}`}>{getStateLabel(job.state)}</span>
      </div>

      <dl className="job-meta-grid">
        <div>
          <dt>Started</dt>
          <dd>{formatTime(job.startedAt)}</dd>
        </div>
        <div>
          <dt>Progress</dt>
          <dd>{job.progressLabel ?? getStateLabel(job.state)}</dd>
        </div>
        <div>
          <dt>Finished</dt>
          <dd>{formatTime(job.completedAt)}</dd>
        </div>
      </dl>

      <div className="job-log" role="log" aria-label={`${job.name} log`}>
        {displayedLogs.length > 0 ? (
          <>
            {hiddenLogCount > 0 ? (
              <p className="job-log-truncated">
                <span>...</span>
                Showing latest {displayedLogs.length} lines; {hiddenLogCount} earlier lines hidden.
              </p>
            ) : null}
            {displayedLogs.map((line) => (
              <p key={line.id}>
                <span>{line.timestamp}</span>
                {line.message}
              </p>
            ))}
          </>
        ) : (
          <p>
            <span>--:--</span>
            Waiting for backend log output.
          </p>
        )}
      </div>
    </section>
  );
}
