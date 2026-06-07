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
