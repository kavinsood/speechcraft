import { describe, expect, it } from "vitest";
import {
  initialPipelineSelection,
  pipelineSelectionReducer,
} from "./PipelineContext";

describe("pipelineSelectionReducer", () => {
  it("keeps processing and slicer run selections independent", () => {
    const afterProcessing = pipelineSelectionReducer(initialPipelineSelection, {
      type: "select-processing-run",
      runId: "processing-run-1",
    });
    expect(afterProcessing.selectedProcessingRunId).toBe("processing-run-1");
    expect(afterProcessing.selectedSlicerDatasetRunId).toBeNull();

    const afterSlicer = pipelineSelectionReducer(afterProcessing, {
      type: "select-slicer-dataset-run",
      runId: "slicer-run-1",
    });
    expect(afterSlicer.selectedProcessingRunId).toBe("processing-run-1");
    expect(afterSlicer.selectedSlicerDatasetRunId).toBe("slicer-run-1");
  });

  it("keeps speakers and processing selections independent", () => {
    const afterSpeakers = pipelineSelectionReducer(initialPipelineSelection, {
      type: "select-speakers-run",
      runId: "speakers-run-1",
    });
    expect(afterSpeakers.selectedSpeakersRunId).toBe("speakers-run-1");
    expect(afterSpeakers.selectedProcessingRunId).toBeNull();

    const afterProcessing = pipelineSelectionReducer(afterSpeakers, {
      type: "select-processing-run",
      runId: "processing-run-1",
    });
    expect(afterProcessing.selectedSpeakersRunId).toBe("speakers-run-1");
    expect(afterProcessing.selectedProcessingRunId).toBe("processing-run-1");
  });

  it("does not change slicer selection when processing run changes", () => {
    const seeded = pipelineSelectionReducer(initialPipelineSelection, {
      type: "select-slicer-dataset-run",
      runId: "slicer-run-1",
    });

    const next = pipelineSelectionReducer(seeded, {
      type: "select-processing-run",
      runId: "processing-run-2",
    });

    expect(next.selectedProcessingRunId).toBe("processing-run-2");
    expect(next.selectedSlicerDatasetRunId).toBe("slicer-run-1");
  });
});
