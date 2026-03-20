import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Settings2 } from "lucide-react";
import { ApiError, fetchProjects } from "./api";
import IngestPage from "./pages/IngestPage";
import LabelPage from "./pages/LabelPage";
import ReferencePage from "./pages/ReferencePage";
import StepPlaceholderPage from "./pages/StepPlaceholderPage";
import type { Project } from "./types";

type AppStep = "ingest" | "enhance" | "segment" | "label" | "reference" | "train" | "deploy";
type ProjectLoadStatus = "loading" | "ready" | "error";

type AppRoute = {
  step: AppStep;
  projectId: string | null;
};

type StepDefinition = {
  id: AppStep;
  label: string;
  shortLabel: string;
  glyph: string;
  tone: string;
};

type PageHeaderContent = {
  eyebrow: string;
  title: string;
  description: string;
};

const stepDefinitions: StepDefinition[] = [
  { id: "ingest", label: "Ingest", shortLabel: "In", glyph: "I", tone: "Sources first" },
  { id: "enhance", label: "Enhance", shortLabel: "En", glyph: "E", tone: "Clean the raw signal" },
  { id: "segment", label: "Segment", shortLabel: "Se", glyph: "S", tone: "Split into candidates" },
  { id: "label", label: "Label", shortLabel: "La", glyph: "L", tone: "Human review and repair" },
  { id: "reference", label: "Reference", shortLabel: "Re", glyph: "R", tone: "Mine and curate steering clips" },
  { id: "train", label: "Train", shortLabel: "Tr", glyph: "T", tone: "Fine-tune the voice" },
  { id: "deploy", label: "Deploy", shortLabel: "De", glyph: "D", tone: "Ship for inference" },
];

function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof ApiError) {
    return error.message;
  }

  if (error instanceof Error && error.message.trim()) {
    return error.message;
  }

  return fallback;
}

function isAppStep(value: string | null): value is AppStep {
  return stepDefinitions.some((step) => step.id === value);
}

function readRouteFromLocation(): AppRoute {
  const path = window.location.pathname.replace(/^\/+|\/+$/g, "");
  const maybeStep = path.length > 0 ? path : "ingest";
  const step = isAppStep(maybeStep) ? maybeStep : "ingest";
  const projectId = new URLSearchParams(window.location.search).get("project")?.trim() ?? null;

  return {
    step,
    projectId: projectId && projectId.length > 0 ? projectId : null,
  };
}

function writeRouteToLocation(route: AppRoute, replace = false) {
  const url = new URL(window.location.href);
  url.pathname = route.step === "ingest" ? "/" : `/${route.step}`;
  if (route.projectId) {
    url.searchParams.set("project", route.projectId);
  } else {
    url.searchParams.delete("project");
  }

  if (replace) {
    window.history.replaceState({}, "", url);
    return;
  }

  window.history.pushState({}, "", url);
}

function getStepIndex(step: AppStep): number {
  return stepDefinitions.findIndex((entry) => entry.id === step);
}

function getPageHeaderContent(step: AppStep, activeProject: Project | null): PageHeaderContent {
  if (step === "ingest") {
    return {
      eyebrow: "Step 01",
      title: activeProject?.name ?? "No project selected",
      description:
        "Ingest is the new home base: set up model access, point Speechcraft at source folders, and define where assets will sync next.",
    };
  }

  if (step === "enhance") {
    return {
      eyebrow: "Step 02",
      title: "Enhancement shell",
      description:
        "Prepare denoise, de-echo, de-reverb, and music removal as a focused workstation.",
    };
  }

  if (step === "segment") {
    return {
      eyebrow: "Step 03",
      title: "Segmentation shell",
      description: "Own the split-and-propose stage before manual review begins.",
    };
  }

  if (step === "label") {
    return {
      eyebrow: "Step 04",
      title: activeProject?.name ?? "Label Workstation",
      description:
        "Manual transcript verification, clip correction, and export readiness all live here.",
    };
  }

  if (step === "reference") {
    return {
      eyebrow: "Step 05",
      title: activeProject?.name ?? "Reference Workstation",
      description:
        "Curate reusable steering clips from source recordings and saved slice states without polluting the label queue.",
    };
  }

  if (step === "train") {
    return {
      eyebrow: "Step 06",
      title: "Training shell",
      description: "Stage the fine-tuning workspace without forcing a job model too early.",
    };
  }

  return {
    eyebrow: "Step 07",
    title: "Deployment shell",
    description: "Hold inference, serving, and release actions in a final workstation stage.",
  };
}

export default function App() {
  const [route, setRoute] = useState<AppRoute>(() => readRouteFromLocation());
  const [projectLoadStatus, setProjectLoadStatus] = useState<ProjectLoadStatus>("loading");
  const [projectLoadError, setProjectLoadError] = useState<string | null>(null);
  const [projects, setProjects] = useState<Project[]>([]);
  const [pageHeaderActions, setPageHeaderActions] = useState<ReactNode>(null);

  useEffect(() => {
    const handlePopState = () => {
      setRoute(readRouteFromLocation());
    };

    window.addEventListener("popstate", handlePopState);
    return () => {
      window.removeEventListener("popstate", handlePopState);
    };
  }, []);

  async function loadProjects(preferredProjectId?: string | null) {
    setProjectLoadStatus("loading");
    setProjectLoadError(null);

    try {
      const nextProjects = await fetchProjects();
      const sortedProjects = [...nextProjects].sort(
        (left, right) => new Date(right.updated_at).getTime() - new Date(left.updated_at).getTime(),
      );
      const fallbackProjectId = sortedProjects[0]?.id ?? null;
      const selectedProjectId =
        preferredProjectId && nextProjects.some((project) => project.id === preferredProjectId)
          ? preferredProjectId
          : route.projectId && nextProjects.some((project) => project.id === route.projectId)
            ? route.projectId
            : fallbackProjectId;

      setProjects(sortedProjects);
      setProjectLoadStatus("ready");

      if (selectedProjectId !== route.projectId) {
        const nextRoute = { ...route, projectId: selectedProjectId };
        setRoute(nextRoute);
        writeRouteToLocation(nextRoute, true);
      }
    } catch (error) {
      setProjects([]);
      setProjectLoadStatus("error");
      setProjectLoadError(
        getErrorMessage(error, "The app could not load the project list from the backend."),
      );
    }
  }

  useEffect(() => {
    void loadProjects(route.projectId);
  }, []);

  const activeProject = useMemo(
    () => projects.find((project) => project.id === route.projectId) ?? null,
    [projects, route.projectId],
  );
  const activeStepIndex = getStepIndex(route.step);
  const pageHeaderContent = useMemo(
    () => getPageHeaderContent(route.step, activeProject),
    [route.step, activeProject],
  );

  useEffect(() => {
    setPageHeaderActions(null);
  }, [route.step]);

  function navigate(nextStep: AppStep, nextProjectId = route.projectId) {
    const nextRoute = { step: nextStep, projectId: nextProjectId ?? null };
    setRoute(nextRoute);
    writeRouteToLocation(nextRoute);
  }

  function handleProjectChange(nextProjectId: string) {
    const nextRoute = {
      step: route.step,
      projectId: nextProjectId,
    };
    setRoute(nextRoute);
    writeRouteToLocation(nextRoute);
  }

  const pageProps = {
    activeProject,
    projectLoadStatus,
    projectLoadError,
    onRetryProjects: () => void loadProjects(route.projectId),
  };

  let pageContent = null;

  if (route.step === "ingest") {
    pageContent = <IngestPage {...pageProps} />;
  } else if (route.step === "label") {
    pageContent = <LabelPage {...pageProps} onHeaderActionsChange={setPageHeaderActions} />;
  } else if (route.step === "reference") {
    pageContent = <ReferencePage {...pageProps} />;
  } else {
    const configByStep: Record<Exclude<AppStep, "ingest" | "label" | "reference">, { leftTitle: string; rightTitle: string; leftItems: string[]; rightItems: string[]; centerTitle: string; centerBody: string; }> = {
      enhance: {
        leftTitle: "Planned modules",
        rightTitle: "Notes",
        leftItems: ["Denoise queue", "Dereverb presets", "Music removal routing"],
        rightItems: ["Keep navigation open.", "Do not assume strict gating.", "This page should later reflect real project assets."],
        centerTitle: "Signal cleanup canvas",
        centerBody: "This shell is ready for the enhancement tools. The frame is in place so we can deepen this room without changing the global app structure again.",
      },
      segment: {
        leftTitle: "Inputs",
        rightTitle: "Outputs",
        leftItems: ["Prepared sources", "Segmentation models", "Boundary configuration"],
        rightItems: ["Candidate clips", "Initial transcript alignment", "Queue handoff to label"],
        centerTitle: "Segmentation canvas",
        centerBody: "This page will become the bridge between preprocessing and review. For now it keeps the shape of the workstation while we decide the exact model and job semantics.",
      },
      train: {
        leftTitle: "Training inputs",
        rightTitle: "Artifacts",
        leftItems: ["Accepted dataset", "Model source", "Hyperparameter presets"],
        rightItems: ["Checkpoints", "Metrics", "Evaluation notes"],
        centerTitle: "Training canvas",
        centerBody: "The app shell now has a dedicated room for training. We can wire the CLI-backed flow into this page incrementally instead of rethinking the navigation later.",
      },
      deploy: {
        leftTitle: "Release setup",
        rightTitle: "Destinations",
        leftItems: ["Model selection", "Runtime profiles", "Access controls"],
        rightItems: ["Local inference", "Hosted endpoints", "Publishing history"],
        centerTitle: "Deployment canvas",
        centerBody: "This final stage is intentionally skeletal for now. The shell makes the end-to-end product legible today without pretending the backend orchestration is already designed.",
      },
    };

    const stepConfig = configByStep[route.step];
    pageContent = <StepPlaceholderPage {...pageProps} {...stepConfig} />;
  }

  return (
    <div className="workstation-shell">
      <header className="shell-header">
        <div className="shell-header-grid">
          <div className="chrome-brand">
            <p className="eyebrow">Speechcraft</p>
            <h1>Dataset Workstation</h1>
          </div>

          <div className="step-page-summary">
            <div className="step-page-title-row">
              <p className="eyebrow">{pageHeaderContent.eyebrow}</p>
              <h2>{pageHeaderContent.title}</h2>
            </div>
            <p className="step-page-copy">{pageHeaderContent.description}</p>
          </div>

          <div className="shell-header-tools">
            <div className="chrome-controls">
              {pageHeaderActions ? <div className="step-page-actions">{pageHeaderActions}</div> : null}

              <label className="project-picker" htmlFor="project-picker">
                <span>Project</span>
                <select
                  id="project-picker"
                  value={route.projectId ?? ""}
                  onChange={(event) => handleProjectChange(event.target.value)}
                  disabled={projectLoadStatus !== "ready" || projects.length === 0}
                >
                  {projects.length === 0 ? <option value="">No projects loaded</option> : null}
                  {projects.map((project) => (
                    <option key={project.id} value={project.id}>
                      {project.name}
                    </option>
                  ))}
                </select>
              </label>

              <button className="icon-button utility-link" type="button" aria-label="Settings">
                <Settings2 aria-hidden="true" strokeWidth={2.1} />
              </button>
            </div>
          </div>
        </div>
      </header>

      <section className="workstation-stage">
        {projectLoadError ? <p className="shell-notice shell-notice-error">{projectLoadError}</p> : null}
        {pageContent}
      </section>

      <nav className="resolve-dock" aria-label="Workflow steps">
        <div className="resolve-dock-track" aria-hidden="true">
          <span
            className="resolve-dock-track-progress"
            style={{
              width:
                stepDefinitions.length > 1
                  ? `${(activeStepIndex / (stepDefinitions.length - 1)) * 100}%`
                  : "0%",
            }}
          />
        </div>

        <div className="resolve-dock-items">
          {stepDefinitions.map((step, index) => {
            const state =
              index < activeStepIndex ? "complete" : index === activeStepIndex ? "active" : "idle";

            return (
              <button
                key={step.id}
                type="button"
                className={`dock-item dock-item-${state}`}
                onClick={() => navigate(step.id)}
                aria-current={route.step === step.id ? "page" : undefined}
                title={step.tone}
              >
                <span className="dock-item-glyph">{step.glyph}</span>
                <span className="dock-item-label">{step.label}</span>
                <span className="dock-item-tone" aria-hidden="true">
                  {step.tone}
                </span>
              </button>
            );
          })}
        </div>
      </nav>

      {activeProject ? (
        <footer className="workstation-footer">
          <span>{activeProject.name}</span>
          <span>updated {new Date(activeProject.updated_at).toLocaleDateString()}</span>
          <span>
            export {activeProject.export_status ? activeProject.export_status.replace(/_/g, " ") : "n/a"}
          </span>
          <span>{stepDefinitions[activeStepIndex]?.shortLabel ?? "In"} active</span>
        </footer>
      ) : null}
    </div>
  );
}
