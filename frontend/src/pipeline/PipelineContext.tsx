import { createContext, useContext, type ReactNode } from "react";

export type PipelineStage = "ingest" | "overview" | "slicer" | "qc" | "lab" | "export";

export type QcRunSelection = {
  slicerRunId: string;
  qcRunId: string;
};

export type LabHandoffContext = {
  source: "qc";
  slicerRunId: string;
  qcRunId: string | null;
  bucketFilter: "auto-kept" | "needs-review" | "auto-rejected" | "all";
  sort: "source-order" | "qc-score-ascending" | "qc-score-descending";
  keepThreshold: number | null;
  rejectThreshold: number | null;
  preset: string | null;
};

export type PipelineSelectionState = {
  selectedSlicerRunId: string | null;
  selectedQcRun: QcRunSelection | null;
  labHandoff: LabHandoffContext | null;
};

export type PipelineSelectionContextValue = PipelineSelectionState & {
  selectedQcRunId: string | null;
  selectSlicerRun: (runId: string | null) => void;
  selectQcRun: (qcRunId: string | null) => void;
  setLabHandoff: (handoff: LabHandoffContext | null) => void;
  resetPipelineSelection: () => void;
};

const PipelineContext = createContext<PipelineSelectionContextValue | null>(null);

type PipelineProviderProps = PipelineSelectionContextValue & {
  children: ReactNode;
};

export type PipelineSelectionAction =
  | { type: "select-slicer-run"; runId: string | null }
  | { type: "select-qc-run"; qcRunId: string | null }
  | { type: "set-lab-handoff"; handoff: LabHandoffContext | null }
  | { type: "replace"; state: PipelineSelectionState }
  | { type: "reset" };

export const initialPipelineSelection: PipelineSelectionState = {
  selectedSlicerRunId: null,
  selectedQcRun: null,
  labHandoff: null,
};

export function pipelineSelectionReducer(
  state: PipelineSelectionState,
  action: PipelineSelectionAction,
): PipelineSelectionState {
  if (action.type === "replace") {
    return action.state;
  }

  if (action.type === "reset") {
    return initialPipelineSelection;
  }

  if (action.type === "select-slicer-run") {
    if (action.runId === state.selectedSlicerRunId) {
      return state;
    }

    return {
      selectedSlicerRunId: action.runId,
      selectedQcRun: null,
      labHandoff: null,
    };
  }

  if (action.type === "select-qc-run") {
    if (!state.selectedSlicerRunId || !action.qcRunId) {
      return {
        ...state,
        selectedQcRun: null,
        labHandoff: null,
      };
    }

    const nextSelection = {
      slicerRunId: state.selectedSlicerRunId,
      qcRunId: action.qcRunId,
    };

    return {
      ...state,
      selectedQcRun: nextSelection,
      labHandoff:
        state.labHandoff?.slicerRunId === nextSelection.slicerRunId &&
        state.labHandoff.qcRunId === nextSelection.qcRunId
          ? state.labHandoff
          : null,
    };
  }

  if (!action.handoff) {
    return {
      ...state,
      labHandoff: null,
    };
  }

  const handoffMatchesSelection =
    action.handoff.slicerRunId === state.selectedSlicerRunId &&
    (!action.handoff.qcRunId ||
      (state.selectedQcRun?.slicerRunId === action.handoff.slicerRunId &&
        state.selectedQcRun.qcRunId === action.handoff.qcRunId));

  if (!handoffMatchesSelection) {
    return state;
  }

  return {
    ...state,
    labHandoff: action.handoff,
  };
}

export function PipelineProvider({ children, ...value }: PipelineProviderProps) {
  return <PipelineContext.Provider value={value}>{children}</PipelineContext.Provider>;
}

export function usePipelineContext() {
  const context = useContext(PipelineContext);

  if (!context) {
    throw new Error("usePipelineContext must be used inside PipelineProvider.");
  }

  return context;
}
