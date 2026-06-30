import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";

const sourceRoot = join(process.cwd(), "src");
const bannedPatterns = [
  "fetchProjectSlicerRuns",
  "createProjectSlicerRun",
  "deleteProjectSlicerRun",
  "/slicer-runs",
  "selectedSlicerRunId",
  "selectSlicerRun",
];

function sourceFiles(root: string): string[] {
  return readdirSync(root).flatMap((entry: string) => {
    const path = join(root, entry);
    return statSync(path).isDirectory() ? sourceFiles(path) : [path];
  });
}

describe("pipeline legacy burn contract", () => {
  it("does not expose old project slicer run frontend wiring", () => {
    const offenders = sourceFiles(sourceRoot).flatMap((path) => {
      const content = readFileSync(path, "utf8");
      return bannedPatterns
        .filter((pattern) => content.includes(pattern))
        .map((pattern) => `${relative(sourceRoot, path)} contains ${pattern}`);
    });

    expect(offenders).toEqual([]);
  });
});
