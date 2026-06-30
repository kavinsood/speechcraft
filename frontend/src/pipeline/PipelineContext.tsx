import { createContext, useContext, type ReactNode } from "react";

export type PipelineStage = "ingest" | "overview" | "speakers" | "processing" | "slicer" | "qc" | "lab" | "export";

export type PipelineSelectionState = {
  selectedSpeakersRunId: string | null;
  selectedProcessingRunId: string | null;
  selectedSlicerDatasetRunId: string | null;
  selectedQcDatasetRunId: string | null;
  selectedLabDatasetRunId: string | null;
};

export type PipelineSelectionContextValue = PipelineSelectionState & {
  selectSpeakersRun: (runId: string | null) => void;
  selectProcessingRun: (runId: string | null) => void;
  selectSlicerDatasetRun: (runId: string | null) => void;
  selectQcDatasetRun: (runId: string | null) => void;
  selectLabDatasetRun: (runId: string | null) => void;
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
  | { type: "replace"; state: PipelineSelectionState }
  | { type: "reset" };

export const initialPipelineSelection: PipelineSelectionState = {
  selectedSpeakersRunId: null,
  selectedProcessingRunId: null,
  selectedSlicerDatasetRunId: null,
  selectedQcDatasetRunId: null,
  selectedLabDatasetRunId: null,
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
    };
  }

  if (action.type === "select-lab-dataset-run") {
    if (action.runId === state.selectedLabDatasetRunId) {
      return state;
    }

    return {
      ...state,
      selectedLabDatasetRunId: action.runId,
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
