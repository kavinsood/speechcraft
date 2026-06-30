import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { SliceSummary } from "../types";
import ClipQueuePane from "./ClipQueuePane";
import type { ClipQueueSortMode } from "./workspace-helpers";

function makeSlice(
  id: string,
  transcriptText: string,
  {
    durationSeconds,
    transcriptMatch,
    speakerCheck,
    originalStart,
  }: {
    durationSeconds: number;
    transcriptMatch: number;
    speakerCheck: number;
    originalStart: number;
  },
): SliceSummary {
  return {
    id,
    source_recording_id: "recording-a",
    status: "unresolved",
    duration_seconds: durationSeconds,
    model_metadata: {
      transcript_match: transcriptMatch,
      speaker_check: speakerCheck,
      original_start_time: originalStart,
      original_end_time: originalStart + durationSeconds,
    },
    created_at: "2026-01-01T00:00:00.000Z",
    transcript: {
      id: `${id}-transcript`,
      slice_id: id,
      original_text: transcriptText,
      modified_text: null,
      is_modified: false,
    },
    tags: [],
    can_undo: false,
    can_redo: false,
  };
}

function renderedQueueIds(): string[] {
  return Array.from(document.querySelectorAll(".clip-list-item")).map((element) =>
    element.getAttribute("data-clip-id") ?? "",
  );
}

afterEach(() => {
  cleanup();
});

describe("ClipQueuePane sorting", () => {
  it("keeps rendered order and reported visible ids in sync across sort changes", async () => {
    const clips = [
      makeSlice("alpha", "alpha clip", {
        durationSeconds: 3,
        transcriptMatch: 95,
        speakerCheck: 95,
        originalStart: 0,
      }),
      makeSlice("bravo", "bravo clip", {
        durationSeconds: 9,
        transcriptMatch: 40,
        speakerCheck: 95,
        originalStart: 10,
      }),
      makeSlice("charlie", "charlie clip", {
        durationSeconds: 6,
        transcriptMatch: 80,
        speakerCheck: 80,
        originalStart: 20,
      }),
    ];

    const onVisibleClipIdsChange = vi.fn();

    function Harness() {
      const [sortMode, setSortMode] = useState<ClipQueueSortMode>("source_timeline");

      return (
        <ClipQueuePane
          workspacePhase="ready"
          workspaceError={null}
          workspaceEmptyMessage={null}
          recordings={[]}
          clips={clips}
          activeClipItem={null}
          sortMode={sortMode}
          onSortModeChange={setSortMode}
          onSelectClipItem={() => {}}
          onRetryLoad={() => {}}
          onVisibleClipIdsChange={onVisibleClipIdsChange}
        />
      );
    }

    render(<Harness />);

    expect(renderedQueueIds()).toEqual(["alpha", "bravo", "charlie"]);
    await waitFor(() => {
      expect(onVisibleClipIdsChange).toHaveBeenLastCalledWith(["alpha", "bravo", "charlie"]);
    });

    const select = screen.getByRole("combobox", { name: "Sort clips" });

    fireEvent.change(select, { target: { value: "longest_first" } });
    await waitFor(() => {
      expect(renderedQueueIds()).toEqual(["bravo", "charlie", "alpha"]);
      expect(onVisibleClipIdsChange).toHaveBeenLastCalledWith(["bravo", "charlie", "alpha"]);
    });

    fireEvent.change(select, { target: { value: "transcript_confidence" } });
    await waitFor(() => {
      expect(renderedQueueIds()).toEqual(["bravo", "charlie", "alpha"]);
      expect(onVisibleClipIdsChange).toHaveBeenLastCalledWith(["bravo", "charlie", "alpha"]);
    });

    fireEvent.change(select, { target: { value: "best_reference_candidate" } });
    await waitFor(() => {
      expect(renderedQueueIds()).toEqual(["charlie", "alpha", "bravo"]);
      expect(onVisibleClipIdsChange).toHaveBeenLastCalledWith(["charlie", "alpha", "bravo"]);
    });

    fireEvent.change(select, { target: { value: "source_timeline" } });
    await waitFor(() => {
      expect(renderedQueueIds()).toEqual(["alpha", "bravo", "charlie"]);
      expect(onVisibleClipIdsChange).toHaveBeenLastCalledWith(["alpha", "bravo", "charlie"]);
    });
  });
});
