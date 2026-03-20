import { useEffect, useMemo, useRef, useState } from "react";
import {
  ApiError,
  buildReferenceVariantAudioUrl,
  fetchProjectReferenceAssets,
  fetchProjectSourceRecordings,
  fetchReferenceAsset,
} from "../api";
import type {
  Project,
  ReferenceAssetDetail,
  ReferenceAssetSummary,
  SourceRecording,
} from "../types";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type ReferencePageProps = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
};

type PageStatus = "loading" | "ready" | "error";

function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof ApiError) {
    return error.message;
  }

  if (error instanceof Error && error.message.trim()) {
    return error.message;
  }

  return fallback;
}

function formatDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return "0.0s";
  }
  return `${seconds.toFixed(seconds >= 10 ? 1 : 2)}s`;
}

function readSelectedAssetIdFromLocation(): string | null {
  const assetId = new URLSearchParams(window.location.search).get("asset")?.trim() ?? null;
  return assetId && assetId.length > 0 ? assetId : null;
}

export default function ReferencePage({
  activeProject,
  projectLoadStatus,
  projectLoadError,
  onRetryProjects,
}: ReferencePageProps) {
  const [pageStatus, setPageStatus] = useState<PageStatus>("loading");
  const [pageError, setPageError] = useState<string | null>(null);
  const [sourceRecordings, setSourceRecordings] = useState<SourceRecording[]>([]);
  const [referenceAssets, setReferenceAssets] = useState<ReferenceAssetSummary[]>([]);
  const [selectedAssetId, setSelectedAssetId] = useState<string | null>(null);
  const [selectedAsset, setSelectedAsset] = useState<ReferenceAssetDetail | null>(null);
  const latestLoadRequestRef = useRef(0);
  const latestDetailRequestRef = useRef(0);

  async function loadPage(projectId: string | null | undefined) {
    const requestId = latestLoadRequestRef.current + 1;
    latestLoadRequestRef.current = requestId;
    setPageStatus("loading");
    setPageError(null);

    if (!projectId) {
      setSourceRecordings([]);
      setReferenceAssets([]);
      setSelectedAssetId(null);
      setSelectedAsset(null);
      setPageStatus("ready");
      return;
    }

    try {
      const [nextRecordings, nextAssets] = await Promise.all([
        fetchProjectSourceRecordings(projectId),
        fetchProjectReferenceAssets(projectId),
      ]);

      if (latestLoadRequestRef.current !== requestId) {
        return;
      }

      const assetIdFromLocation = readSelectedAssetIdFromLocation();
      setSourceRecordings(nextRecordings);
      setReferenceAssets(nextAssets);
      setSelectedAssetId((current) =>
        current && nextAssets.some((asset) => asset.id === current)
          ? current
          : assetIdFromLocation && nextAssets.some((asset) => asset.id === assetIdFromLocation)
            ? assetIdFromLocation
            : (nextAssets[0]?.id ?? null),
      );
      setPageStatus("ready");
    } catch (error) {
      if (latestLoadRequestRef.current !== requestId) {
        return;
      }

      setSourceRecordings([]);
      setReferenceAssets([]);
      setSelectedAssetId(null);
      setSelectedAsset(null);
      setPageStatus("error");
      setPageError(
        getErrorMessage(error, "The reference workstation failed to load. Check the backend and try again."),
      );
    }
  }

  useEffect(() => {
    if (projectLoadStatus === "error") {
      setPageStatus("error");
      setPageError(projectLoadError ?? "The project list failed to load.");
      return;
    }

    if (projectLoadStatus === "loading") {
      setPageStatus("loading");
      return;
    }

    void loadPage(activeProject?.id);
  }, [activeProject?.id, projectLoadError, projectLoadStatus]);

  useEffect(() => {
    const requestId = latestDetailRequestRef.current + 1;
    latestDetailRequestRef.current = requestId;

    if (!selectedAssetId) {
      setSelectedAsset(null);
      return;
    }

    void (async () => {
      try {
        const detail = await fetchReferenceAsset(selectedAssetId);
        if (latestDetailRequestRef.current !== requestId) {
          return;
        }
        setSelectedAsset(detail);
      } catch (error) {
        if (latestDetailRequestRef.current !== requestId) {
          return;
        }
        setSelectedAsset(null);
        setPageError(getErrorMessage(error, "The selected reference could not be loaded."));
      }
    })();
  }, [selectedAssetId]);

  useEffect(() => {
    const url = new URL(window.location.href);
    if (selectedAssetId) {
      url.searchParams.set("asset", selectedAssetId);
    } else {
      url.searchParams.delete("asset");
    }
    window.history.replaceState({}, "", url);
  }, [selectedAssetId]);

  const activeVariant = selectedAsset?.active_variant ?? null;
  const activeVariantAudioUrl = useMemo(() => {
    if (!activeVariant) {
      return null;
    }
    return buildReferenceVariantAudioUrl(activeVariant.id);
  }, [activeVariant]);

  return (
    <section className="step-page">
      {projectLoadStatus === "error" ? (
        <WorkspaceStatePanel
          title="Projects unavailable"
          message={projectLoadError ?? "The project list could not be loaded."}
          actionLabel="Retry project load"
          onAction={onRetryProjects}
        />
      ) : (
        <div className="stage-layout">
          <aside className="stage-sidebar panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Project sources</p>
                <h3>Candidate input</h3>
              </div>
            </div>

            {pageStatus === "loading" ? <div className="empty-state">Loading source recordings...</div> : null}
            {pageStatus === "error" ? (
              <WorkspaceStatePanel
                title="Sources unavailable"
                message={pageError ?? "The source recording list could not be loaded."}
                actionLabel="Retry load"
                onAction={() => void loadPage(activeProject?.id)}
              />
            ) : null}
            {pageStatus === "ready" && sourceRecordings.length === 0 ? (
              <div className="empty-state">This project does not have any source recordings yet.</div>
            ) : null}
            {pageStatus === "ready" && sourceRecordings.length > 0 ? (
              <ul className="stage-list">
                {sourceRecordings.map((recording) => (
                  <li key={recording.id}>
                    <strong>{recording.id}</strong>
                    <span>
                      {formatDuration(recording.duration_seconds)} • {recording.sample_rate / 1000} kHz
                      {recording.processing_recipe ? ` • ${recording.processing_recipe}` : " • original"}
                    </span>
                  </li>
                ))}
              </ul>
            ) : null}
          </aside>

          <main className="stage-main">
            <section className="panel stage-placeholder-hero">
              <p className="eyebrow">Reference workstation</p>
              <h3>Phase 1 foundation is live.</h3>
              <p>
                The real schema, project source list, and shared reference library are wired. Candidate
                runs, reranking, and trim-first promotion are the next layer on top of this page.
              </p>
            </section>

            <section className="panel transcript-panel">
              <div className="panel-header">
                <div>
                  <p className="eyebrow">Selected asset</p>
                  <h3>{selectedAsset?.name ?? "Reference preview"}</h3>
                </div>
              </div>

              {pageStatus === "loading" ? <div className="empty-state">Loading references...</div> : null}
              {pageStatus === "ready" && !selectedAsset ? (
                <div className="empty-state">
                  Save the current slice state as a reference from the label workstation to start filling this library.
                </div>
              ) : null}
              {selectedAsset ? (
                <div className="commit-list">
                  <div className="commit-card selected">
                    <div className="commit-row">
                      <strong>{selectedAsset.name}</strong>
                      <span>{selectedAsset.status}</span>
                    </div>
                    <p>{selectedAsset.transcript_text || "No transcript stored on this reference yet."}</p>
                    <span className="commit-time">
                      {selectedAsset.speaker_name || "speaker n/a"}
                      {selectedAsset.language ? ` • ${selectedAsset.language}` : ""}
                      {activeVariant ? ` • ${formatDuration(activeVariant.num_samples / Math.max(activeVariant.sample_rate, 1))}` : ""}
                    </span>
                  </div>

                  {activeVariantAudioUrl ? (
                    <audio controls preload="none" src={activeVariantAudioUrl}>
                      <track kind="captions" />
                    </audio>
                  ) : null}

                  <div className="stats-table">
                    <div className="stats-row">
                      <span>Asset id</span>
                      <span />
                      <span>{selectedAsset.id}</span>
                    </div>
                    <div className="stats-row">
                      <span>Variants</span>
                      <span />
                      <span>{selectedAsset.variants.length}</span>
                    </div>
                    <div className="stats-row">
                      <span>Created</span>
                      <span />
                      <span>{new Date(selectedAsset.created_at).toLocaleString()}</span>
                    </div>
                  </div>
                </div>
              ) : null}
            </section>
          </main>

          <aside className="stage-sidebar panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Reference library</p>
                <h3>Saved assets</h3>
              </div>
            </div>

            {pageStatus === "ready" && referenceAssets.length === 0 ? (
              <div className="empty-state">No saved references yet.</div>
            ) : null}

            {referenceAssets.length > 0 ? (
              <div className="commit-list">
                {referenceAssets.map((asset) => (
                  <button
                    key={asset.id}
                    type="button"
                    className={`commit-card ${asset.id === selectedAssetId ? "selected" : ""}`}
                    onClick={() => setSelectedAssetId(asset.id)}
                  >
                    <div className="commit-row">
                      <strong>{asset.name}</strong>
                      <span>{asset.status}</span>
                    </div>
                    <p>{asset.transcript_text || "No transcript preview"}</p>
                    <span className="commit-time">
                      {asset.speaker_name || "speaker n/a"}
                      {asset.active_variant
                        ? ` • ${formatDuration(
                            asset.active_variant.num_samples / Math.max(asset.active_variant.sample_rate, 1),
                          )}`
                        : ""}
                    </span>
                  </button>
                ))}
              </div>
            ) : null}
          </aside>
        </div>
      )}
    </section>
  );
}
