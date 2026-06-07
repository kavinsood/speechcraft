import { createContext, useContext, type ReactNode } from "react";

export type PipelineStage = "ingest" | "overview" | "speakers" | "processing" | "slicer" | "qc" | "lab" | "export";

export type QcRunSelection = {
  datasetRunId: string;
  qcRunId: string;
};

export type LabHandoffContext = {
  source: "qc";
  datasetRunId: string;
  qcRunId: string | null;
  bucketFilter: "auto-kept" | "needs-review" | "auto-rejected" | "all";
  sort: "source-order" | "qc-score-ascending" | "qc-score-descending";
  keepThreshold: number | null;
  rejectThreshold: number | null;
  preset: string | null;
};

export type PipelineSelectionState = {
  selectedSpeakersRunId: string | null;
  selectedProcessingRunId: string | null;
  selectedSlicerDatasetRunId: string | null;
  selectedQcDatasetRunId: string | null;
  selectedLabDatasetRunId: string | null;
  selectedQcRun: QcRunSelection | null;
  labHandoff: LabHandoffContext | null;
};

export type PipelineSelectionContextValue = PipelineSelectionState & {
  selectedQcRunId: string | null;
  selectSpeakersRun: (runId: string | null) => void;
  selectProcessingRun: (runId: string | null) => void;
  selectSlicerDatasetRun: (runId: string | null) => void;
  selectQcDatasetRun: (runId: string | null) => void;
  selectLabDatasetRun: (runId: string | null) => void;
  selectQcRun: (qcRunId: string | null) => void;
  setLabHandoff: (handoff: LabHandoffContext | null) => void;
  resetPipelineSelection: () => void;
};

const PipelineContext = createContext<PipelineSelectionContextValue | null>(null);

type PipelineProviderProps = PipelineSelectionContextValue & {
  children: ReactNode;
};

export type PipelineSelectionAction =
  | { type: "select-speakers-run"; runId: string | null }
  | { type: "select-processing-run"; runId: string | null }
  | { type: "select-slicer-dataset-run"; runId: string | null }
  | { type: "select-qc-dataset-run"; runId: string | null }
  | { type: "select-lab-dataset-run"; runId: string | null }
  | { type: "select-qc-run"; qcRunId: string | null }
  | { type: "set-lab-handoff"; handoff: LabHandoffContext | null }
  | { type: "replace"; state: PipelineSelectionState }
  | { type: "reset" };

export const initialPipelineSelection: PipelineSelectionState = {
  selectedSpeakersRunId: null,
  selectedProcessingRunId: null,
  selectedSlicerDatasetRunId: null,
  selectedQcDatasetRunId: null,
  selectedLabDatasetRunId: null,
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

  if (action.type === "select-processing-run") {
    if (action.runId === state.selectedProcessingRunId) {
      return state;
    }

    return {
      ...state,
      selectedProcessingRunId: action.runId,
    };
  }

  if (action.type === "select-speakers-run") {
    if (action.runId === state.selectedSpeakersRunId) {
      return state;
    }

    return {
      ...state,
      selectedSpeakersRunId: action.runId,
    };
  }

  if (action.type === "select-slicer-dataset-run") {
    if (action.runId === state.selectedSlicerDatasetRunId) {
      return state;
    }

    return {
      ...state,
      selectedSlicerDatasetRunId: action.runId,
    };
  }

  if (action.type === "select-qc-dataset-run") {
    if (action.runId === state.selectedQcDatasetRunId) {
      return state;
    }

    return {
      ...state,
      selectedQcDatasetRunId: action.runId,
      selectedQcRun: null,
      labHandoff: null,
    };
  }

  if (action.type === "select-lab-dataset-run") {
    if (action.runId === state.selectedLabDatasetRunId) {
      return state;
    }

    return {
      ...state,
      selectedLabDatasetRunId: action.runId,
      labHandoff: null,
    };
  }

  if (action.type === "select-qc-run") {
    if (!state.selectedQcDatasetRunId || !action.qcRunId) {
      return {
        ...state,
        selectedQcRun: null,
        labHandoff: null,
      };
    }

    const nextSelection = {
      datasetRunId: state.selectedQcDatasetRunId,
      qcRunId: action.qcRunId,
    };

    return {
      ...state,
      selectedQcRun: nextSelection,
      labHandoff:
        state.labHandoff?.datasetRunId === nextSelection.datasetRunId &&
        state.labHandoff.qcRunId === nextSelection.qcRunId
          ? state.labHandoff
          : null,
    };
  }

  if (action.type === "set-lab-handoff") {
    if (!action.handoff) {
      return {
        ...state,
        labHandoff: null,
      };
    }

    const handoffMatchesSelection =
      action.handoff.datasetRunId === state.selectedLabDatasetRunId &&
      (!action.handoff.qcRunId ||
        (state.selectedQcRun?.datasetRunId === action.handoff.datasetRunId &&
          state.selectedQcRun.qcRunId === action.handoff.qcRunId));

    if (!handoffMatchesSelection) {
      return state;
    }

    return {
      ...state,
      labHandoff: action.handoff,
    };
  }

  return state;
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
