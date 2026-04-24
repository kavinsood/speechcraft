import { useEffect, useMemo, useReducer, useState, type ReactNode } from "react";
import { Settings2 } from "lucide-react";
import { ApiError, fetchProjects } from "./api";
import {
  PipelineProvider,
  initialPipelineSelection,
  pipelineSelectionReducer,
  type LabHandoffContext,
  type PipelineSelectionState,
  type PipelineStage,
} from "./pipeline/PipelineContext";
import ExportPage from "./pages/ExportPage";
import IngestPage from "./pages/IngestPage";
import LabPage from "./pages/LabPage";
import OverviewPage from "./pages/OverviewPage";
import QcPage from "./pages/QcPage";
import ReferencePage from "./pages/ReferencePage";
import SlicerPage from "./pages/SlicerPage";
import type { ClipLabItemRef, Project } from "./types";

type AppStep = PipelineStage | "reference";
type ProjectLoadStatus = "loading" | "ready" | "error";

type AppRoute = {
  step: AppStep;
  projectId: string | null;
  clipItem: ClipLabItemRef | null;
};

type AppLocationState = {
  route: AppRoute;
  pipelineSelection: PipelineSelectionState;
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

const qcBucketFilters = ["auto-kept", "needs-review", "auto-rejected", "all"] as const;
const qcSortModes = ["source-order", "qc-score-ascending", "qc-score-descending"] as const;

const stepDefinitions: StepDefinition[] = [
  { id: "ingest", label: "Ingest", shortLabel: "In", glyph: "I", tone: "Sources first" },
  { id: "overview", label: "Overview", shortLabel: "Ov", glyph: "O", tone: "Raw recordings and prep" },
  { id: "slicer", label: "Slicer", shortLabel: "Sl", glyph: "S", tone: "Create candidate slice runs" },
  { id: "qc", label: "QC", shortLabel: "QC", glyph: "Q", tone: "Machine triage for one run" },
  { id: "lab", label: "Lab", shortLabel: "La", glyph: "L", tone: "Human review and override" },
  { id: "export", label: "Export", shortLabel: "Ex", glyph: "E", tone: "Emit training-ready data" },
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
  return value === "reference" || stepDefinitions.some((step) => step.id === value);
}

function isQcBucketFilter(value: string | null): value is LabHandoffContext["bucketFilter"] {
  return qcBucketFilters.some((bucket) => bucket === value);
}

function isQcSortMode(value: string | null): value is LabHandoffContext["sort"] {
  return qcSortModes.some((sort) => sort === value);
}

function parseNullableThreshold(value: string | null): number | null {
  if (!value) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function readPipelineSelectionFromSearch(
  searchParams: URLSearchParams,
  step: AppStep,
): PipelineSelectionState {
  const runId = searchParams.get("run")?.trim() || null;
  const qcRunId = searchParams.get("qc")?.trim() || null;
  const bucket = searchParams.get("bucket");
  const sort = searchParams.get("sort");
  const keepThreshold = parseNullableThreshold(searchParams.get("keep") ?? searchParams.get("threshold"));
  const rejectThreshold = parseNullableThreshold(searchParams.get("reject"));
  const preset = searchParams.get("preset")?.trim() || null;
  const selectedQcRun = runId && qcRunId ? { slicerRunId: runId, qcRunId } : null;

  if (!runId) {
    return initialPipelineSelection;
  }

  const hasHandoffParams =
    step === "lab" ||
    isQcBucketFilter(bucket) ||
    isQcSortMode(sort) ||
    keepThreshold !== null ||
    rejectThreshold !== null ||
    preset !== null;

  return {
    selectedSlicerRunId: runId,
    selectedQcRun,
    labHandoff: hasHandoffParams
      ? {
          source: "qc",
          slicerRunId: runId,
          qcRunId,
          bucketFilter: isQcBucketFilter(bucket) ? bucket : "all",
          sort: isQcSortMode(sort) ? sort : "source-order",
          keepThreshold,
          rejectThreshold,
          preset,
        }
      : null,
  };
}

function readLocationState(): AppLocationState {
  const path = window.location.pathname.replace(/^\/+|\/+$/g, "");
  const legacyStepMap: Partial<Record<string, AppStep>> = {
    enhance: "overview",
    segment: "slicer",
    label: "lab",
    train: "export",
    deploy: "export",
  };
  const maybeStep = path.length > 0 ? legacyStepMap[path] ?? path : "ingest";
  const step = isAppStep(maybeStep) ? maybeStep : "ingest";
  const searchParams = new URLSearchParams(window.location.search);
  const projectId = searchParams.get("project")?.trim() ?? null;
  const clipId = searchParams.get("clip_id")?.trim() ?? null;
  const clipItem = clipId ? ({ id: clipId } satisfies ClipLabItemRef) : null;

  return {
    route: {
      step,
      projectId: projectId && projectId.length > 0 ? projectId : null,
      clipItem,
    },
    pipelineSelection: readPipelineSelectionFromSearch(searchParams, step),
  };
}

function writeRouteToLocation(
  route: AppRoute,
  pipelineSelection: PipelineSelectionState,
  replace = false,
) {
  const url = new URL(window.location.href);
  url.pathname = route.step === "ingest" ? "/" : `/${route.step}`;
  if (route.projectId) {
    url.searchParams.set("project", route.projectId);
  } else {
    url.searchParams.delete("project");
  }
  if (route.clipItem) {
    url.searchParams.set("clip_id", route.clipItem.id);
  } else {
    url.searchParams.delete("clip_id");
  }

  const routeUsesRunSelection = route.step === "slicer" || route.step === "qc" || route.step === "lab";
  const routeUsesQcSelection = route.step === "qc" || route.step === "lab";
  const routeUsesLabHandoff = route.step === "lab";

  if (routeUsesRunSelection && pipelineSelection.selectedSlicerRunId) {
    url.searchParams.set("run", pipelineSelection.selectedSlicerRunId);
  } else {
    url.searchParams.delete("run");
  }

  if (routeUsesQcSelection && pipelineSelection.selectedQcRun) {
    url.searchParams.set("qc", pipelineSelection.selectedQcRun.qcRunId);
  } else {
    url.searchParams.delete("qc");
  }

  if (routeUsesLabHandoff && pipelineSelection.labHandoff) {
    url.searchParams.set("bucket", pipelineSelection.labHandoff.bucketFilter);
    url.searchParams.set("sort", pipelineSelection.labHandoff.sort);
    if (pipelineSelection.labHandoff.keepThreshold !== null) {
      url.searchParams.set("keep", String(pipelineSelection.labHandoff.keepThreshold));
    } else {
      url.searchParams.delete("keep");
    }
    if (pipelineSelection.labHandoff.rejectThreshold !== null) {
      url.searchParams.set("reject", String(pipelineSelection.labHandoff.rejectThreshold));
    } else {
      url.searchParams.delete("reject");
    }
    if (pipelineSelection.labHandoff.preset) {
      url.searchParams.set("preset", pipelineSelection.labHandoff.preset);
    } else {
      url.searchParams.delete("preset");
    }
    url.searchParams.delete("threshold");
  } else {
    url.searchParams.delete("bucket");
    url.searchParams.delete("sort");
    url.searchParams.delete("keep");
    url.searchParams.delete("reject");
    url.searchParams.delete("preset");
    url.searchParams.delete("threshold");
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

  if (step === "overview") {
    return {
      eyebrow: "Step 02",
      title: "Overview",
      description: "Inspect imported raw recordings and manage explicit preparation state.",
    };
  }

  if (step === "slicer") {
    return {
      eyebrow: "Step 03",
      title: "Slicer",
      description: "Create and inspect candidate slice runs over prepared recordings.",
    };
  }

  if (step === "qc") {
    return {
      eyebrow: "Step 04",
      title: "QC",
      description: "Run machine triage for one selected slicer run without claiming human approval.",
    };
  }

  if (step === "lab") {
    return {
      eyebrow: "Step 05",
      title: activeProject?.name ?? "Lab",
      description: "Manual slice review, transcript repair, and human overrides live here.",
    };
  }

  if (step === "reference") {
    return {
      eyebrow: "Reference",
      title: activeProject?.name ?? "Reference Workstation",
      description:
        "Reference remains available as its existing workstation, outside the current sprint path.",
    };
  }

  return {
    eyebrow: "Step 06",
    title: "Export",
    description: "Emit the selected reviewed or machine-triaged dataset for downstream training.",
  };
}

export default function App() {
  const [route, setRoute] = useState<AppRoute>(() => readLocationState().route);
  const [projectLoadStatus, setProjectLoadStatus] = useState<ProjectLoadStatus>("loading");
  const [projectLoadError, setProjectLoadError] = useState<string | null>(null);
  const [projects, setProjects] = useState<Project[]>([]);
  const [pageHeaderActions, setPageHeaderActions] = useState<ReactNode>(null);
  const [pipelineSelection, dispatchPipelineSelection] = useReducer(
    pipelineSelectionReducer,
    undefined,
    () => readLocationState().pipelineSelection,
  );

  useEffect(() => {
    const handlePopState = () => {
      const nextLocationState = readLocationState();
      setRoute(nextLocationState.route);
      dispatchPipelineSelection({ type: "replace", state: nextLocationState.pipelineSelection });
    };

    window.addEventListener("popstate", handlePopState);
    return () => {
      window.removeEventListener("popstate", handlePopState);
    };
  }, []);

  async function loadProjects(preferredProjectId?: string | null, routeSnapshot = route) {
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
          : routeSnapshot.projectId && nextProjects.some((project) => project.id === routeSnapshot.projectId)
            ? routeSnapshot.projectId
            : fallbackProjectId;

      setProjects(sortedProjects);
      setProjectLoadStatus("ready");

      if (selectedProjectId !== routeSnapshot.projectId) {
        dispatchPipelineSelection({ type: "reset" });
        const nextRoute = { ...routeSnapshot, projectId: selectedProjectId };
        setRoute(nextRoute);
        writeRouteToLocation(nextRoute, initialPipelineSelection, true);
      }
    } catch (error) {
      setProjects([]);
      setProjectLoadStatus("error");
      dispatchPipelineSelection({ type: "reset" });
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
  const visibleStepIndex = activeStepIndex >= 0 ? activeStepIndex : 0;
  const pageHeaderContent = useMemo(
    () => getPageHeaderContent(route.step, activeProject),
    [route.step, activeProject],
  );

  useEffect(() => {
    setPageHeaderActions(null);
  }, [route.step]);

  function navigate(nextStep: AppStep, nextProjectId = route.projectId) {
    let nextPipelineSelection = pipelineSelection;
    if ((nextProjectId ?? null) !== route.projectId) {
      nextPipelineSelection = initialPipelineSelection;
      dispatchPipelineSelection({ type: "reset" });
    }

    const nextRoute = { step: nextStep, projectId: nextProjectId ?? null, clipItem: route.clipItem };
    setRoute(nextRoute);
    writeRouteToLocation(nextRoute, nextPipelineSelection);
  }

  function handleProjectChange(nextProjectId: string) {
    dispatchPipelineSelection({ type: "reset" });
    const nextRoute = {
      step: route.step,
      projectId: nextProjectId,
      clipItem: route.clipItem,
    };
    setRoute(nextRoute);
    writeRouteToLocation(nextRoute, initialPipelineSelection);
  }

  function selectSlicerRun(runId: string | null) {
    const nextPipelineSelection: PipelineSelectionState = {
      selectedSlicerRunId: runId,
      selectedQcRun: null,
      labHandoff: null,
    };
    dispatchPipelineSelection({ type: "select-slicer-run", runId });
    writeRouteToLocation(route, nextPipelineSelection, true);
  }

  function selectQcRun(qcRunId: string | null) {
    const nextPipelineSelection: PipelineSelectionState =
      pipelineSelection.selectedSlicerRunId && qcRunId
        ? {
            ...pipelineSelection,
            selectedQcRun: {
              slicerRunId: pipelineSelection.selectedSlicerRunId,
              qcRunId,
            },
            labHandoff: null,
          }
        : {
            ...pipelineSelection,
            selectedQcRun: null,
            labHandoff: null,
          };
    dispatchPipelineSelection({ type: "select-qc-run", qcRunId });
    writeRouteToLocation(route, nextPipelineSelection, true);
  }

  function setPipelineLabHandoff(handoff: LabHandoffContext | null) {
    const handoffMatchesSelection =
      !handoff ||
      (handoff.slicerRunId === pipelineSelection.selectedSlicerRunId &&
        (!handoff.qcRunId ||
          (pipelineSelection.selectedQcRun?.slicerRunId === handoff.slicerRunId &&
            pipelineSelection.selectedQcRun.qcRunId === handoff.qcRunId)));

    if (!handoffMatchesSelection) {
      return;
    }

    const nextPipelineSelection = {
      ...pipelineSelection,
      labHandoff: handoff,
    };
    dispatchPipelineSelection({ type: "set-lab-handoff", handoff });
    writeRouteToLocation(route, nextPipelineSelection, true);
  }

  function handleImportComplete(projectId: string) {
    dispatchPipelineSelection({ type: "reset" });
    const nextRoute = { step: "overview" as const, projectId, clipItem: null };
    setRoute(nextRoute);
    writeRouteToLocation(nextRoute, initialPipelineSelection, true);
    void loadProjects(projectId, nextRoute);
  }

  const pageProps = {
    activeProject,
    projectLoadStatus,
    projectLoadError,
    onRetryProjects: () => void loadProjects(route.projectId),
  };

  let pageContent = null;

  if (route.step === "ingest") {
    pageContent = <IngestPage {...pageProps} onImportComplete={handleImportComplete} />;
  } else if (route.step === "overview") {
    pageContent = <OverviewPage {...pageProps} />;
  } else if (route.step === "slicer") {
    pageContent = <SlicerPage {...pageProps} onOpenQc={() => navigate("qc")} />;
  } else if (route.step === "qc") {
    pageContent = (
      <QcPage
        {...pageProps}
        onOpenLab={(handoff) => {
          const nextPipelineSelection = {
            ...pipelineSelection,
            labHandoff: handoff,
          };
          const nextRoute = { step: "lab" as const, projectId: route.projectId, clipItem: route.clipItem };
          dispatchPipelineSelection({ type: "set-lab-handoff", handoff });
          setRoute(nextRoute);
          writeRouteToLocation(nextRoute, nextPipelineSelection);
        }}
      />
    );
  } else if (route.step === "lab") {
    pageContent = (
      <LabPage
        {...pageProps}
        activeClipItem={route.clipItem}
        onActiveClipItemChange={(clipItem) => {
          const nextRoute = { ...route, clipItem };
          setRoute(nextRoute);
          writeRouteToLocation(nextRoute, pipelineSelection, true);
        }}
        onHeaderActionsChange={setPageHeaderActions}
      />
    );
  } else if (route.step === "reference") {
    pageContent = <ReferencePage {...pageProps} />;
  } else {
    pageContent = <ExportPage {...pageProps} />;
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
        <PipelineProvider
          selectedSlicerRunId={pipelineSelection.selectedSlicerRunId}
          selectedQcRun={pipelineSelection.selectedQcRun}
          selectedQcRunId={pipelineSelection.selectedQcRun?.qcRunId ?? null}
          labHandoff={pipelineSelection.labHandoff}
          selectSlicerRun={selectSlicerRun}
          selectQcRun={selectQcRun}
          setLabHandoff={setPipelineLabHandoff}
          resetPipelineSelection={() => {
            dispatchPipelineSelection({ type: "reset" });
            writeRouteToLocation(route, initialPipelineSelection, true);
          }}
        >
          {pageContent}
        </PipelineProvider>
      </section>

      <nav className="resolve-dock" aria-label="Workflow steps">
        <div className="resolve-dock-track" aria-hidden="true">
          <span
            className="resolve-dock-track-progress"
            style={{
              width:
                stepDefinitions.length > 1
                  ? `${(visibleStepIndex / (stepDefinitions.length - 1)) * 100}%`
                  : "0%",
            }}
          />
        </div>

        <div className="resolve-dock-items">
          {stepDefinitions.map((step, index) => {
            const state =
              index < visibleStepIndex ? "complete" : index === visibleStepIndex ? "active" : "idle";

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
          <span>{stepDefinitions[visibleStepIndex]?.shortLabel ?? "In"} active</span>
        </footer>
      ) : null}
    </div>
  );
}
