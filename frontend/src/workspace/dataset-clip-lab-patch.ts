import type {
  DatasetClipLabAudioOperationRequest,
  DatasetClipLabAudioStackRequest,
  DatasetClipLabClipRow,
  DatasetClipLabPatchRequest,
  DatasetClipLabView,
} from "../types";
import { ApiError } from "../api";

export type ClipPatchBuilder = (
  current: DatasetClipLabClipRow,
) => Omit<DatasetClipLabPatchRequest, "expected_manifest_sha256" | "expected_clip_version">;

export function clipLabPatchQueueKey(runId: string, clipId: string): string {
  return `${runId}:${clipId}`;
}

export function applyDatasetClipLabRow(
  current: DatasetClipLabView | null,
  runId: string,
  updated: DatasetClipLabClipRow,
): DatasetClipLabView | null {
  if (!current || current.run_id !== runId) {
    return current;
  }

  const hasClip = current.clips.some((clip) => clip.clip_id === updated.clip_id);
  return {
    ...current,
    clips: hasClip
      ? current.clips.map((clip) => (clip.clip_id === updated.clip_id ? updated : clip))
      : [...current.clips, updated],
  };
}

export function buildReviewerTagSuggestions(tags: string[]): string[] {
  const byNormalized = new Map<string, string>();

  for (const tag of tags) {
    const trimmed = tag.trim();
    const normalized = trimmed.toLocaleLowerCase();
    if (normalized && !byNormalized.has(normalized)) {
      byNormalized.set(normalized, trimmed);
    }
  }

  return [...byNormalized.values()].sort((left, right) => left.localeCompare(right));
}

export function filterTagSuggestions(
  suggestions: string[],
  input: string,
  reviewerTags: string[],
  machineLabels: string[],
): string[] {
  const query = input.trim().toLocaleLowerCase();
  const taken = new Set([
    ...reviewerTags.map((tag) => tag.toLocaleLowerCase()),
    ...machineLabels.map((label) => label.toLocaleLowerCase()),
  ]);

  return suggestions.filter((tagName) => {
    const normalized = tagName.toLocaleLowerCase();
    if (taken.has(normalized)) {
      return false;
    }
    if (!query) {
      return true;
    }
    return normalized.includes(query);
  });
}

type PatchClipFn = (
  runId: string,
  clipId: string,
  payload: DatasetClipLabPatchRequest,
) => Promise<DatasetClipLabClipRow>;

type AppendAudioOpFn = (
  runId: string,
  clipId: string,
  payload: DatasetClipLabAudioOperationRequest,
) => Promise<DatasetClipLabClipRow>;

type AudioStackFn = (
  runId: string,
  clipId: string,
  payload: DatasetClipLabAudioStackRequest,
) => Promise<DatasetClipLabClipRow>;

type CreateClipLabPatchCoordinatorOptions = {
  getView: () => DatasetClipLabView | null;
  patchClip: PatchClipFn;
  appendAudioOp?: AppendAudioOpFn;
  undoAudio?: AudioStackFn;
  redoAudio?: AudioStackFn;
  onViewChange: (next: DatasetClipLabView) => void;
  onRowUpdated: (runId: string, updated: DatasetClipLabClipRow) => void;
  onConflict: (runId: string, clipId: string) => Promise<void>;
};

function requireClipRow(view: DatasetClipLabView, clipId: string): DatasetClipLabClipRow {
  const current = view.clips.find((clip) => clip.clip_id === clipId);
  if (!current) {
    throw new Error("Clip Lab row is no longer available.");
  }
  return current;
}

export function createClipLabPatchCoordinator({
  getView,
  patchClip,
  appendAudioOp,
  undoAudio,
  redoAudio,
  onViewChange,
  onRowUpdated,
  onConflict,
}: CreateClipLabPatchCoordinatorOptions) {
  const queues: Record<string, Promise<unknown>> = {};
  let generation = 0;

  function commitView(runId: string, updated: DatasetClipLabClipRow): boolean {
    const current = getView();
    const next = applyDatasetClipLabRow(current, runId, updated);
    if (!next || next === current) {
      return false;
    }
    onViewChange(next);
    onRowUpdated(runId, updated);
    return true;
  }

  function patchDatasetClipLab(
    runId: string,
    clipId: string,
    buildPatch: ClipPatchBuilder,
  ): Promise<DatasetClipLabClipRow> {
    const queueKey = clipLabPatchQueueKey(runId, clipId);
    const previous = queues[queueKey] ?? Promise.resolve();
    const operationGeneration = generation;
    const next = previous.then(async () => {
      if (operationGeneration !== generation) {
        throw new Error("Clip Lab edit was cancelled after the dataset run changed.");
      }

      const view = getView();
      if (!view || view.run_id !== runId) {
        throw new Error("Dataset run changed before the edit could be saved.");
      }

      const current = requireClipRow(view, clipId);

      try {
        const updated = await patchClip(runId, clipId, {
          expected_manifest_sha256: view.candidate_manifest_sha256,
          expected_clip_version: current.clip_version,
          ...buildPatch(current),
        });
        if (operationGeneration !== generation) {
          return updated;
        }
        commitView(runId, updated);
        return updated;
      } catch (error) {
        if (isConflictError(error)) {
          await onConflict(runId, clipId);
        }
        throw error;
      }
    });

    queues[queueKey] = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  }

  function mutateDatasetClipLabAudio(
    runId: string,
    clipId: string,
    mutate: (
      view: DatasetClipLabView,
      current: DatasetClipLabClipRow,
    ) => Promise<DatasetClipLabClipRow>,
  ): Promise<DatasetClipLabClipRow> {
    const queueKey = clipLabPatchQueueKey(runId, clipId);
    const previous = queues[queueKey] ?? Promise.resolve();
    const operationGeneration = generation;
    const next = previous.then(async () => {
      if (operationGeneration !== generation) {
        throw new Error("Clip Lab edit was cancelled after the dataset run changed.");
      }

      const view = getView();
      if (!view || view.run_id !== runId) {
        throw new Error("Dataset run changed before the edit could be saved.");
      }

      const current = requireClipRow(view, clipId);

      try {
        const updated = await mutate(view, current);
        if (operationGeneration !== generation) {
          return updated;
        }
        commitView(runId, updated);
        return updated;
      } catch (error) {
        if (isConflictError(error)) {
          await onConflict(runId, clipId);
        }
        throw error;
      }
    });

    queues[queueKey] = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  }

  function appendDatasetAudioOperation(
    runId: string,
    clipId: string,
    operation: DatasetClipLabAudioOperationRequest["operation"],
  ): Promise<DatasetClipLabClipRow> {
    if (!appendAudioOp) {
      throw new Error("Dataset audio operations are unavailable.");
    }
    return mutateDatasetClipLabAudio(runId, clipId, async (view, current) =>
      appendAudioOp(runId, clipId, {
        expected_manifest_sha256: view.candidate_manifest_sha256,
        expected_clip_version: current.clip_version,
        operation,
      }),
    );
  }

  function undoDatasetAudioOperation(
    runId: string,
    clipId: string,
  ): Promise<DatasetClipLabClipRow> {
    if (!undoAudio) {
      throw new Error("Dataset audio undo is unavailable.");
    }
    return mutateDatasetClipLabAudio(runId, clipId, async (view, current) =>
      undoAudio(runId, clipId, {
        expected_manifest_sha256: view.candidate_manifest_sha256,
        expected_clip_version: current.clip_version,
      }),
    );
  }

  function redoDatasetAudioOperation(
    runId: string,
    clipId: string,
  ): Promise<DatasetClipLabClipRow> {
    if (!redoAudio) {
      throw new Error("Dataset audio redo is unavailable.");
    }
    return mutateDatasetClipLabAudio(runId, clipId, async (view, current) =>
      redoAudio(runId, clipId, {
        expected_manifest_sha256: view.candidate_manifest_sha256,
        expected_clip_version: current.clip_version,
      }),
    );
  }

  function resetQueues() {
    generation += 1;
    for (const key of Object.keys(queues)) {
      delete queues[key];
    }
  }

  return {
    patchDatasetClipLab,
    appendDatasetAudioOperation,
    undoDatasetAudioOperation,
    redoDatasetAudioOperation,
    resetQueues,
    commitView,
  };
}

function isConflictError(error: unknown): boolean {
  return error instanceof ApiError && error.status === 409;
}
