import { act, render, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

type DeferredLoad = {
  resolve: () => void;
  reject: (error: Error) => void;
};

const deferredLoads: DeferredLoad[] = [];
const readyOnceHandlers: Array<() => void> = [];
const waveSurferLoad = vi.fn(() =>
  new Promise<void>((resolve, reject) => {
    deferredLoads.push({
      resolve: () => {
        for (const handler of readyOnceHandlers.splice(0, readyOnceHandlers.length)) {
          handler();
        }
        resolve();
      },
      reject,
    });
  }),
);
const waveSurferDestroy = vi.fn();
const waveSurferOn = vi.fn();
const waveSurferSeekTo = vi.fn();
const waveSurferGetDuration = vi.fn(() => 2);
const waveSurferGetCurrentTime = vi.fn(() => 0);
const waveSurferGetWrapper = vi.fn(() => ({
  parentElement: null,
  scrollWidth: 100,
}));

vi.mock("wavesurfer.js", () => ({
  default: {
    create: vi.fn(() => ({
      load: waveSurferLoad,
      destroy: waveSurferDestroy,
      on: waveSurferOn,
      once: vi.fn((event: string, handler: () => void) => {
        if (event === "ready") {
          readyOnceHandlers.push(handler);
        }
      }),
      seekTo: waveSurferSeekTo,
      getDuration: waveSurferGetDuration,
      getCurrentTime: waveSurferGetCurrentTime,
      getWrapper: waveSurferGetWrapper,
      isPlaying: vi.fn(() => false),
      zoom: vi.fn(),
    })),
  },
}));

const enableDragSelection = vi.fn();

vi.mock("wavesurfer.js/dist/plugins/regions.esm.js", () => ({
  default: {
    create: vi.fn(() => ({
      enableDragSelection,
      on: vi.fn(),
      getRegions: vi.fn(() => []),
      addRegion: vi.fn(),
    })),
  },
}));

import WaveformPane from "./WaveformPane";

async function resolveAllPendingLoads() {
  const pending = [...deferredLoads];
  await act(async () => {
    for (const load of pending) {
      load.resolve();
    }
    await Promise.resolve();
  });
}

describe("WaveformPane", () => {
  beforeEach(() => {
    deferredLoads.length = 0;
    readyOnceHandlers.length = 0;
    waveSurferLoad.mockClear();
    waveSurferSeekTo.mockClear();
    enableDragSelection.mockClear();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("does not load dataset audio before peaks resolve on first mount", async () => {
    const revisionKey = "rev-a";
    const peaks = [0.1, 0.2, 0.3];

    const { rerender } = render(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/a.wav"
        durationSeconds={2}
        peaks={null}
        loadRevisionKey={revisionKey}
        peaksLoadState="loading"
        requirePeaksBeforeLoad
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await act(async () => {
      await Promise.resolve();
    });
    expect(waveSurferLoad).not.toHaveBeenCalled();

    rerender(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/a.wav"
        durationSeconds={2}
        peaks={peaks}
        loadRevisionKey={revisionKey}
        peaksLoadState="ready"
        requirePeaksBeforeLoad
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await waitFor(() => {
      expect(waveSurferLoad).toHaveBeenCalledTimes(1);
    });
    await resolveAllPendingLoads();
    expect(waveSurferLoad).toHaveBeenCalledWith(
      "http://127.0.0.1:8010/media/a.wav",
      [peaks],
      2,
    );
  });

  it("blocks idle dataset mounts until peaks are ready", async () => {
    render(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/a.wav"
        durationSeconds={2}
        peaks={null}
        loadRevisionKey="rev-a"
        peaksLoadState="idle"
        requirePeaksBeforeLoad
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await act(async () => {
      await Promise.resolve();
    });
    expect(waveSurferLoad).not.toHaveBeenCalled();
  });

  it("loads each revision once when switching clips", async () => {
    const peaksA = [0.1, 0.2];
    const peaksB = [0.3, 0.4];

    const { rerender } = render(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/a.wav"
        durationSeconds={2}
        peaks={null}
        loadRevisionKey="rev-a"
        peaksLoadState="loading"
        requirePeaksBeforeLoad
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    rerender(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/a.wav"
        durationSeconds={2}
        peaks={peaksA}
        loadRevisionKey="rev-a"
        peaksLoadState="ready"
        requirePeaksBeforeLoad
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await waitFor(() => {
      expect(waveSurferLoad).toHaveBeenCalledTimes(1);
    });
    await resolveAllPendingLoads();

    rerender(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/b.wav"
        durationSeconds={3}
        peaks={peaksA}
        loadRevisionKey="rev-b"
        peaksLoadState="loading"
        requirePeaksBeforeLoad
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await act(async () => {
      await Promise.resolve();
    });
    expect(waveSurferLoad).toHaveBeenCalledTimes(1);

    rerender(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/b.wav"
        durationSeconds={3}
        peaks={peaksB}
        loadRevisionKey="rev-b"
        peaksLoadState="ready"
        requirePeaksBeforeLoad
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await waitFor(() => {
      expect(waveSurferLoad).toHaveBeenCalledTimes(2);
    });
    await resolveAllPendingLoads();
    expect(waveSurferLoad).toHaveBeenLastCalledWith(
      "http://127.0.0.1:8010/media/b.wav",
      [peaksB],
      3,
    );
  });

  it("falls back to one audio-only load when dataset peaks fail", async () => {
    const { rerender } = render(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/a.wav"
        durationSeconds={2}
        peaks={null}
        loadRevisionKey="rev-a"
        peaksLoadState="loading"
        requirePeaksBeforeLoad
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    rerender(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/a.wav"
        durationSeconds={2}
        peaks={null}
        loadRevisionKey="rev-a"
        peaksLoadState="failed"
        requirePeaksBeforeLoad
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await waitFor(() => {
      expect(waveSurferLoad).toHaveBeenCalledTimes(1);
    });
    await resolveAllPendingLoads();
    expect(waveSurferLoad).toHaveBeenCalledWith(
      "http://127.0.0.1:8010/media/a.wav",
      undefined,
      2,
    );

    rerender(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/a.wav"
        durationSeconds={2}
        peaks={null}
        loadRevisionKey="rev-a"
        peaksLoadState="failed"
        requirePeaksBeforeLoad
        desiredCursorSeconds={1.1}
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await act(async () => {
      await Promise.resolve();
    });
    expect(waveSurferLoad).toHaveBeenCalledTimes(1);
  });

  it("does not reload when only the playhead changes", async () => {
    const revisionKey = "rev-a";
    const peaks = [0.5, 0.4];

    const { rerender } = render(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/a.wav"
        durationSeconds={2}
        peaks={peaks}
        loadRevisionKey={revisionKey}
        peaksLoadState="ready"
        desiredCursorSeconds={0}
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await waitFor(() => {
      expect(waveSurferLoad).toHaveBeenCalledTimes(1);
    });
    await resolveAllPendingLoads();

    rerender(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/a.wav"
        durationSeconds={2}
        peaks={peaks}
        loadRevisionKey={revisionKey}
        peaksLoadState="ready"
        desiredCursorSeconds={1.25}
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await act(async () => {
      await Promise.resolve();
    });

    expect(waveSurferLoad).toHaveBeenCalledTimes(1);
  });

  it("does not reload or re-seek when peaks flicker after the revision is loaded", async () => {
    const revisionKey = "rev-a";
    const peaks = [0.5, 0.4];

    const { rerender } = render(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/a.wav"
        durationSeconds={2}
        peaks={peaks}
        loadRevisionKey={revisionKey}
        peaksLoadState="ready"
        requirePeaksBeforeLoad
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await waitFor(() => {
      expect(waveSurferLoad).toHaveBeenCalledTimes(1);
    });
    await resolveAllPendingLoads();
    expect(waveSurferSeekTo).toHaveBeenCalledTimes(1);

    rerender(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/a.wav"
        durationSeconds={2}
        peaks={null}
        loadRevisionKey={revisionKey}
        peaksLoadState="loading"
        requirePeaksBeforeLoad
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await act(async () => {
      await Promise.resolve();
    });

    rerender(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/a.wav"
        durationSeconds={2}
        peaks={peaks}
        loadRevisionKey={revisionKey}
        peaksLoadState="ready"
        requirePeaksBeforeLoad
        desiredCursorSeconds={1.25}
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await act(async () => {
      await Promise.resolve();
    });

    expect(waveSurferLoad).toHaveBeenCalledTimes(1);
    expect(waveSurferSeekTo).toHaveBeenCalledTimes(1);
  });

  it("ignores stale load completion when an older revision resolves late", async () => {
    const peaksA = [0.1, 0.2];
    const peaksB = [0.3, 0.4];

    const { rerender } = render(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/a.wav"
        durationSeconds={2}
        peaks={peaksA}
        loadRevisionKey="rev-a"
        peaksLoadState="ready"
        requirePeaksBeforeLoad
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await waitFor(() => {
      expect(waveSurferLoad).toHaveBeenCalledTimes(1);
    });
    const loadA = deferredLoads[0];
    expect(loadA).toBeDefined();

    rerender(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/b.wav"
        durationSeconds={3}
        peaks={null}
        loadRevisionKey="rev-b"
        peaksLoadState="loading"
        requirePeaksBeforeLoad
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await act(async () => {
      await Promise.resolve();
    });
    expect(waveSurferLoad).toHaveBeenCalledTimes(1);

    rerender(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/b.wav"
        durationSeconds={3}
        peaks={peaksB}
        loadRevisionKey="rev-b"
        peaksLoadState="ready"
        requirePeaksBeforeLoad
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await waitFor(() => {
      expect(waveSurferLoad).toHaveBeenCalledTimes(2);
    });
    const loadB = deferredLoads[1];
    expect(loadB).toBeDefined();

    await act(async () => {
      loadB.resolve();
      await Promise.resolve();
    });

    await act(async () => {
      loadA.resolve();
      await Promise.resolve();
    });

    rerender(
      <WaveformPane
        audioUrl="http://127.0.0.1:8010/media/b.wav"
        durationSeconds={3}
        peaks={peaksB}
        loadRevisionKey="rev-b"
        peaksLoadState="ready"
        requirePeaksBeforeLoad
        desiredCursorSeconds={1.1}
        selectionStart={0}
        selectionEnd={0}
        onSelectionChange={() => {}}
        onCursorChange={() => {}}
      />,
    );

    await act(async () => {
      await Promise.resolve();
    });

    expect(waveSurferLoad).toHaveBeenCalledTimes(2);
    expect(waveSurferLoad).toHaveBeenLastCalledWith(
      "http://127.0.0.1:8010/media/b.wav",
      [peaksB],
      3,
    );
  });
});
