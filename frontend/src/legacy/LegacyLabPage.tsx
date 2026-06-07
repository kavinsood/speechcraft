import { type ReactNode } from "react";
import { usePipelineContext } from "../pipeline/PipelineContext";
import type { ClipLabItemRef, Project } from "../types";
import LabelPage from "./LabelPage";

type LabPageProps = {
  activeProject: Project | null;
  activeClipItem: ClipLabItemRef | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onActiveClipItemChange: (clipItem: ClipLabItemRef | null) => void;
  onRetryProjects: () => void;
  onHeaderActionsChange: (actions: ReactNode) => void;
};

export default function LabPage(props: LabPageProps) {
  const { labHandoff } = usePipelineContext();

  return (
    <section className="lab-page-shell">
      {labHandoff ? (
        <div className="pipeline-handoff-banner">
          <strong>QC handoff</strong>
          <span>
            Run {labHandoff.slicerRunId}, filter {labHandoff.bucketFilter.replace(/-/g, " ")}, sort{" "}
            {labHandoff.sort.replace(/-/g, " ")}, keep {labHandoff.keepThreshold ?? "n/a"}, reject{" "}
            {labHandoff.rejectThreshold ?? "n/a"}, preset {labHandoff.preset ?? "n/a"}
          </span>
        </div>
      ) : null}
      <LabelPage {...props} />
    </section>
  );
}
