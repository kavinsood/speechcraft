import type { Project } from "../types";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type IngestPageProps = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
};

export default function IngestPage({
  activeProject,
  projectLoadStatus,
  projectLoadError,
  onRetryProjects,
}: IngestPageProps) {
  return (
    <section className="step-page ingest-page">
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
                <p className="eyebrow">Project setup</p>
                <h3>Sources</h3>
              </div>
            </div>
            <ul className="stage-list">
              <li>
                <strong>Project folder</strong>
                <span>Drop datasets into the workspace directory created for this project.</span>
              </li>
              <li>
                <strong>Local assets</strong>
                <span>Keep all raw takes, references, and manifests anchored here first.</span>
              </li>
              <li>
                <strong>Docs</strong>
                <span>Link the ingest step to the workflow docs once the first actions are wired.</span>
              </li>
            </ul>
          </aside>

          <main className="stage-main">
            <section className="panel ingest-hero">
              <p className="eyebrow">Ingest home</p>
              <h3>Bring raw material into the pipeline.</h3>
              <p>
                This shell is ready for the first real workstation page. The main actions are already
                framed around the three entry points you called out, without pretending the final
                backend model is settled yet.
              </p>
            </section>

            <section className="ingest-action-grid">
              <article className="panel ingest-action-card">
                <p className="eyebrow">Models</p>
                <h3>Login to Hugging Face</h3>
                <p>Authenticate once and let Speechcraft pull the models needed for each stage.</p>
                <button className="primary-button" type="button">
                  Connect account
                </button>
              </article>

              <article className="panel ingest-action-card">
                <p className="eyebrow">Local sources</p>
                <h3>Drop datasets into the project folder</h3>
                <p>
                  We will expose the project path here and turn this area into the handoff point for
                  raw audio, manifests, and related files.
                </p>
                <button className="ghost-button" type="button">
                  Reveal folder
                </button>
              </article>

              <article className="panel ingest-action-card">
                <p className="eyebrow">Cloud sync</p>
                <h3>Connect storage for upload and backup</h3>
                <p>Keep remote destinations optional, but available from the first step of the flow.</p>
                <button className="ghost-button" type="button">
                  Add cloud target
                </button>
              </article>
            </section>
          </main>

          <aside className="stage-sidebar panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Next up</p>
                <h3>What this page should teach</h3>
              </div>
            </div>
            <ul className="stage-list">
              <li>
                <strong>What happens first</strong>
                <span>Get the user into a project and show where raw data belongs immediately.</span>
              </li>
              <li>
                <strong>What comes next</strong>
                <span>Make the handoff to enhancement obvious through the bottom dock.</span>
              </li>
              <li>
                <strong>What stays optional</strong>
                <span>Cloud and model auth should support the flow, not block it.</span>
              </li>
            </ul>
          </aside>
        </div>
      )}
    </section>
  );
}
