import JobActivityPanel from "../components/JobActivityPanel";
import type { Project } from "../types";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type ExportPageProps = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
};

export default function ExportPage({
  activeProject,
  projectLoadStatus,
  projectLoadError,
  onRetryProjects,
}: ExportPageProps) {
  if (projectLoadStatus === "error") {
    return (
      <WorkspaceStatePanel
        title="Projects unavailable"
        message={projectLoadError ?? "The project list could not be loaded."}
        actionLabel="Retry project load"
        onAction={onRetryProjects}
      />
    );
  }

  if (projectLoadStatus === "loading") {
    return <WorkspaceStatePanel title="Loading projects" message="Fetching project context." />;
  }

  if (!activeProject) {
    return (
      <WorkspaceStatePanel
        title="No project selected"
        message="Select a project before exporting a reviewed or auto-selected dataset."
      />
    );
  }

  return (
    <section className="step-page pipeline-page export-page">
      <div className="stage-layout">
        <aside className="stage-sidebar panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Export scope</p>
              <h3>Dataset handoff</h3>
            </div>
          </div>
          <ul className="stage-list">
            <li>
              <strong>Selected result set</strong>
              <span>Fast-path auto-selected data or Lab-reviewed data will be chosen here.</span>
            </li>
            <li>
              <strong>Artifacts</strong>
              <span>Manifests and output folders remain an export concern.</span>
            </li>
          </ul>
        </aside>

        <main className="stage-main">
          <section className="panel pipeline-hero">
            <p className="eyebrow">Export shell</p>
            <h3>{activeProject.name}</h3>
            <p>
              Export is included in the workflow navigation so Phase 1 has a complete stage path.
              The export backend and selection policy remain outside this phase.
            </p>
          </section>

          <JobActivityPanel title="Export activity" job={null} />
        </main>

        <aside className="stage-sidebar panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Does not own</p>
              <h3>Boundaries</h3>
            </div>
          </div>
          <ul className="stage-list">
            <li>
              <strong>QC scoring</strong>
              <span>Export consumes selected sets; it does not recalculate QC.</span>
            </li>
            <li>
              <strong>Human overrides</strong>
              <span>Lab remains the authority for manual review decisions.</span>
            </li>
          </ul>
        </aside>
      </div>
    </section>
  );
}
