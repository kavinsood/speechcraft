import type { Project } from "../types";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type StepPlaceholderPageProps = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
  leftTitle: string;
  rightTitle: string;
  leftItems: string[];
  rightItems: string[];
  centerTitle: string;
  centerBody: string;
};

export default function StepPlaceholderPage({
  activeProject,
  projectLoadStatus,
  projectLoadError,
  onRetryProjects,
  leftTitle,
  rightTitle,
  leftItems,
  rightItems,
  centerTitle,
  centerBody,
}: StepPlaceholderPageProps) {
  return (
    <section className="step-page">
      {projectLoadStatus === "error" ? (
        <WorkspaceStatePanel
          title="Projects unavailable"
          message={projectLoadError ?? "The project list could not be loaded."}
          actionLabel="Retry project load"
          onAction={onRetryProjects}
        />
      ) : (
        <div className="stage-layout">
          <aside className="stage-sidebar panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">{activeProject ? activeProject.name : "No project"}</p>
                <h3>{leftTitle}</h3>
              </div>
            </div>
            <ul className="stage-list">
              {leftItems.map((item) => (
                <li key={item}>
                  <strong>{item}</strong>
                  <span>Placeholder shell for this step.</span>
                </li>
              ))}
            </ul>
          </aside>

          <main className="stage-main">
            <section className="panel stage-placeholder-hero">
              <p className="eyebrow">Workstation shell</p>
              <h3>{centerTitle}</h3>
              <p>{centerBody}</p>
            </section>
          </main>

          <aside className="stage-sidebar panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Page guide</p>
                <h3>{rightTitle}</h3>
              </div>
            </div>
            <ul className="stage-list">
              {rightItems.map((item) => (
                <li key={item}>
                  <strong>{item}</strong>
                  <span>Reserved for per-step controls and context.</span>
                </li>
              ))}
            </ul>
          </aside>
        </div>
      )}
    </section>
  );
}
