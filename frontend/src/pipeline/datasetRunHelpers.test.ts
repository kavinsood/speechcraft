import { describe, expect, it } from "vitest";

import { resolveLabDatasetRunId } from "./datasetRunHelpers";
import type { DatasetRun } from "../types";

function run(id: string, status: DatasetRun["status"], completedAt: string | null = null): DatasetRun {
  return {
    id,
    project_id: "project-1",
    pipeline_version: "pretraining_rfc_v1",
    artifact_root: `dataset-runs/project-1/${id}`,
    stage: "candidate_clips",
    status,
    config_hash: null,
    input_summary: {},
    output_summary: {},
    reason_codes: [],
    created_at: "2026-06-01T00:00:00Z",
    started_at: null,
    completed_at: completedAt,
    artifacts: [{ id: `${id}-manifest`, kind: "candidate_review_manifest_json", path: "artifacts/candidate_review_manifest.json", summary: {}, reason_codes: [] }],
  };
}

describe("resolveLabDatasetRunId", () => {
  const runs = [
    run("dataset-old", "completed", "2026-06-01T00:00:00Z"),
    run("dataset-new", "completed", "2026-06-05T00:00:00Z"),
    run("dataset-running", "running", null),
  ];

  it("prefers the lab selection when eligible", () => {
    expect(resolveLabDatasetRunId(runs, ["dataset-old", null, null])).toBe("dataset-old");
  });

  it("falls back to slicer then qc context", () => {
    expect(resolveLabDatasetRunId(runs, [null, "dataset-running", null])).toBe("dataset-running");
  });

  it("defaults to the newest completed eligible run", () => {
    expect(resolveLabDatasetRunId(runs, [null, null, null])).toBe("dataset-new");
  });

  it("returns null when no eligible runs exist", () => {
    expect(resolveLabDatasetRunId([], [null, null, null])).toBeNull();
  });
});
