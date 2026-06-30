export type PeaksLoadStatus = "idle" | "loading" | "ready" | "failed";

export type RevisionPeaksState = {
  revisionKey: string | null;
  status: PeaksLoadStatus;
  peaks: number[] | null;
};

export const EMPTY_REVISION_PEAKS: RevisionPeaksState = {
  revisionKey: null,
  status: "idle",
  peaks: null,
};

export function initialRevisionPeaksForFetch(revisionKey: string): RevisionPeaksState {
  return {
    revisionKey,
    status: "loading",
    peaks: null,
  };
}

export function derivePeaksLoadStateForRevision(
  revisionPeaks: RevisionPeaksState,
  activeRevisionKey: string | null,
  options: {
    waveformPeaksUrl: string | null;
    datasetRenderStatus: string | null;
  },
): PeaksLoadStatus {
  if (!activeRevisionKey) {
    return "idle";
  }
  if (options.waveformPeaksUrl && options.datasetRenderStatus === "pending") {
    return "loading";
  }
  if (revisionPeaks.revisionKey !== activeRevisionKey) {
    return "loading";
  }
  return revisionPeaks.status;
}

export function derivePeaksForActiveRevision(
  revisionPeaks: RevisionPeaksState,
  activeRevisionKey: string | null,
): number[] | null {
  if (!activeRevisionKey || revisionPeaks.revisionKey !== activeRevisionKey) {
    return null;
  }
  return revisionPeaks.peaks;
}
