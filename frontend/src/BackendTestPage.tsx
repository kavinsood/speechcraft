import { useMemo, useState } from "react";
import {
  API_BASE,
  appendClipEdlOperation,
  fetchExportPreview,
  fetchHealthStrict,
  fetchProjectExports,
  fetchProjectSlices,
  mergeWithNextClip,
  redoClip,
  runClipLabModel,
  runProjectExport,
  setActiveVariant,
  splitClip,
  undoClip,
  updateClipStatus,
  updateClipTags,
  updateClipTranscript,
} from "./api";
import type { Slice } from "./types";

type LogEntry = {
  step: string;
  status: "idle" | "running" | "ok" | "error";
  detail: string;
};

const demoProjectId = "phase1-demo";

function formatResult(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

export default function BackendTestPage() {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [isRunning, setIsRunning] = useState(false);
  const [slices, setSlices] = useState<Slice[]>([]);

  const activeClipId = useMemo(() => {
    return slices[0]?.id ?? "clip-001";
  }, [slices]);

  function pushLog(step: string, status: LogEntry["status"], detail: string) {
    setLogs((current) => [...current, { step, status, detail }]);
  }

  async function runFullBackendTest() {
    setIsRunning(true);
    setLogs([]);

    try {
      pushLog("Health Check", "running", "Requesting /healthz");
      const health = await fetchHealthStrict();
      pushLog("Health Check", "ok", formatResult(health));

      pushLog("Slice Load", "running", `Requesting /api/projects/${demoProjectId}/slices`);
      const loadedSlices = await fetchProjectSlices(demoProjectId);
      setSlices(loadedSlices);
      pushLog("Slice Load", "ok", formatResult(loadedSlices));

      const clipId = loadedSlices[0]?.id ?? "clip-001";

      pushLog("Status Update", "running", `Setting ${clipId} to quarantined`);
      const statusResult = await updateClipStatus(clipId, "quarantined");
      pushLog("Status Update", "ok", formatResult(statusResult));

      const transcriptSuffix = `[backend-test ${new Date().toLocaleTimeString()}]`;
      pushLog("Transcript Update", "running", `Appending test marker to ${clipId}`);
      const transcriptResult = await updateClipTranscript(
        clipId,
        `${statusResult.transcript?.modified_text ?? statusResult.transcript?.original_text ?? ""} ${transcriptSuffix}`.trim(),
      );
      pushLog("Transcript Update", "ok", formatResult(transcriptResult));

      pushLog("Tag Update", "running", `Saving backend-test tags on ${clipId}`);
      const tagResult = await updateClipTags(clipId, [
        { name: "backend-test", color: "#2f6c8f" },
        { name: "qa", color: "#8a7a3d" },
      ]);
      pushLog("Tag Update", "ok", formatResult(tagResult));

      pushLog("EDL Append", "running", `Appending insert_silence to ${clipId}`);
      const edlResult = await appendClipEdlOperation(clipId, {
        op: "insert_silence",
        duration_seconds: 0.15,
      });
      pushLog("EDL Append", "ok", formatResult(edlResult));

      pushLog("Undo", "running", `Undoing the last edit for ${clipId}`);
      const undoResult = await undoClip(clipId);
      pushLog("Undo", "ok", formatResult(undoResult));

      pushLog("Redo", "running", `Redoing the last edit for ${clipId}`);
      const redoResult = await redoClip(clipId);
      pushLog("Redo", "ok", formatResult(redoResult));

      pushLog("Run Variant", "running", `Running DeepFilterNet on ${clipId}`);
      const variantResult = await runClipLabModel(clipId, "deepfilternet");
      pushLog("Run Variant", "ok", formatResult(variantResult));

      const originalVariantId = variantResult.variants.find((variant) => variant.is_original)?.id;
      if (originalVariantId) {
        pushLog("Variant Switch", "running", `Switching ${clipId} back to ${originalVariantId}`);
        const switched = await setActiveVariant(clipId, originalVariantId);
        pushLog("Variant Switch", "ok", formatResult(switched));
      }

      pushLog("Split Slice", "running", `Splitting ${clipId}`);
      const splitResult = await splitClip(clipId, 0.1);
      pushLog("Split Slice", "ok", formatResult(splitResult));

      const mergeCandidate = splitResult[0]?.id;
      if (mergeCandidate) {
        pushLog("Merge Next Slice", "running", `Merging ${mergeCandidate} with the next active slice`);
        const mergeResult = await mergeWithNextClip(mergeCandidate);
        pushLog("Merge Next Slice", "ok", formatResult(mergeResult));
      }

      pushLog("Export Preview", "running", `Requesting export preview for ${demoProjectId}`);
      const exportPreview = await fetchExportPreview(demoProjectId);
      pushLog("Export Preview", "ok", formatResult(exportPreview));

      pushLog("Run Export", "running", `Rendering export for ${demoProjectId}`);
      const exportRun = await runProjectExport(demoProjectId);
      pushLog("Run Export", "ok", formatResult(exportRun));

      pushLog("Export Runs", "running", `Fetching export runs for ${demoProjectId}`);
      const exportRuns = await fetchProjectExports(demoProjectId);
      pushLog("Export Runs", "ok", formatResult(exportRuns));

      const refreshedSlices = await fetchProjectSlices(demoProjectId);
      setSlices(refreshedSlices);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown backend test failure";
      pushLog("Backend Test", "error", message);
    } finally {
      setIsRunning(false);
    }
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Speechcraft</p>
          <h1>Backend Test Route</h1>
        </div>
        <div className="topbar-actions">
          <a className="status-pill route-link" href="/">
            Back To Workstation
          </a>
          <button className="primary-button" type="button" onClick={runFullBackendTest} disabled={isRunning}>
            {isRunning ? "Running..." : "Run Full Backend Test"}
          </button>
        </div>
      </header>

      <main className="backend-test-layout">
        <section className="panel backend-test-summary">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Target</p>
              <h2>Live Backend</h2>
            </div>
          </div>
          <p className="muted-copy">
            This route calls the real backend only. No local fallback data is used here.
          </p>
          <dl className="backend-test-meta">
            <div>
              <dt>API Base</dt>
              <dd>{API_BASE}</dd>
            </div>
            <div>
              <dt>Project</dt>
              <dd>{demoProjectId}</dd>
            </div>
            <div>
              <dt>Primary Slice</dt>
              <dd>{activeClipId}</dd>
            </div>
          </dl>
        </section>

        <section className="panel backend-test-log">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Execution Log</p>
              <h2>API Coverage</h2>
            </div>
          </div>

          <div className="test-log-list">
            {logs.length > 0 ? (
              logs.map((log, index) => (
                <article key={`${log.step}-${index}`} className={`test-log-card log-${log.status}`}>
                  <div className="test-log-header">
                    <strong>{log.step}</strong>
                    <span>{log.status}</span>
                  </div>
                  <pre>{log.detail}</pre>
                </article>
              ))
            ) : (
              <div className="empty-state">Run the test to exercise the current backend surface.</div>
            )}
          </div>
        </section>
      </main>
    </div>
  );
}
