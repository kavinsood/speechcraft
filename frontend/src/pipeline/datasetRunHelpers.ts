import type { DatasetRun, DatasetSlicerResults } from "../types";

export function isReadyForSlicerHandoff(run: DatasetRun): boolean {
  return run.status === "completed" && run.stage === "alignment_qc";
}

export function isSlicerInputReady(run: DatasetRun | null): boolean {
  return Boolean(
    run?.artifacts.some((artifact) => artifact.kind === "aligned_words_jsonl") &&
      run?.artifacts.some((artifact) => artifact.kind === "alignment_qc_by_buffer_json"),
  );
}

export function hasCandidateClipArtifacts(
  run: DatasetRun | null,
  results: DatasetSlicerResults | null,
): boolean {
  return Boolean(
    run?.artifacts.some((artifact) => artifact.kind === "candidate_review_manifest_json") ||
      (results?.candidate_review_manifest?.length ?? 0) > 0,
  );
}

export function isEligibleLabDatasetRun(run: DatasetRun): boolean {
  return run.artifacts.some((artifact) => artifact.kind === "candidate_review_manifest_json");
}

export function resolveLabDatasetRunId(
  runs: DatasetRun[],
  preferredRunIds: Array<string | null | undefined>,
): string | null {
  if (runs.length === 0) {
    return null;
  }

  const eligible = new Map(runs.map((run) => [run.id, run]));
  for (const runId of preferredRunIds) {
    if (runId && eligible.has(runId)) {
      return runId;
    }
  }

  const completedRuns = runs
    .filter((run) => run.status === "completed")
    .sort((left, right) => {
      const leftTime = new Date(left.completed_at ?? left.created_at).getTime();
      const rightTime = new Date(right.completed_at ?? right.created_at).getTime();
      return rightTime - leftTime;
    });

  return completedRuns[0]?.id ?? runs[0]?.id ?? null;
}
