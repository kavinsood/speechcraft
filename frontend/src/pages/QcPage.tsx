import { usePipelineContext, type LabHandoffContext } from "../pipeline/PipelineContext";
import type { Project } from "../types";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type QcPageProps = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
  onOpenLab: (handoff: LabHandoffContext) => void;
};

export default function QcPage({
  activeProject,
  projectLoadStatus,
  projectLoadError,
  onRetryProjects,
}: QcPageProps) {
  const { selectedQcDatasetRunId } = usePipelineContext();

  if (projectLoadStatus === "error") {
    return (
      <WorkspaceStatePanel
        title="Projects unavailable"
        message={projectLoadError ?? "Project load failed."}
        actionLabel="Retry"
        onAction={onRetryProjects}
      />
    );
  }
  if (projectLoadStatus === "loading") {
    return <WorkspaceStatePanel title="Loading projects" message="Fetching project context." />;
  }
  if (!activeProject) {
    return <WorkspaceStatePanel title="No project selected" message="Select a project before reviewing candidates." />;
  }

  return (
    <section className="step-page pipeline-page qc-page">
      <div className="panel pipeline-hero">
        <p className="eyebrow">Dataset QC</p>
        <h2>New candidate QC is not wired yet</h2>
        <p>
          Legacy Slice/QCRun triage has been removed from the active pipeline. The next QC implementation must read
          dataset-run candidate artifacts and review decisions, not old source-level slicer rows.
        </p>
        <div className="button-row">
          <span className="status-pill">Selected dataset run: {selectedQcDatasetRunId ?? "none"}</span>
        </div>
      </div>
    </section>
  );
}
