import { useEffect, useMemo, useRef, useState } from "react";
import {
  createReferenceRun,
  fetchProjectReferenceAssets,
  fetchProjectReferenceRuns,
  fetchProjectSourceRecordings,
  fetchReferenceAsset,
  fetchReferenceRun,
  fetchReferenceRunCandidates,
  promoteReferenceCandidate,
} from "../api";
import type {
  Project,
  ReferenceAssetDetail,
  ReferenceAssetSummary,
  ReferenceCandidate,
  ReferenceRun,
  SourceRecording,
} from "../types";
import ReferenceCandidatePane from "../reference/ReferenceCandidatePane";
import ReferenceLibraryPane from "../reference/ReferenceLibraryPane";
import ReferenceRunSidebar from "../reference/ReferenceRunSidebar";
import {
  getReferenceErrorMessage,
  readSelectedAssetIdFromLocation,
  replaceSelectedAssetInLocation,
} from "../reference/reference-helpers";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type ReferencePageProps = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
};

type PageStatus = "loading" | "ready" | "error";

export default function ReferencePage({
  activeProject,
  projectLoadStatus,
  projectLoadError,
  onRetryProjects,
}: ReferencePageProps) {
  const [pageStatus, setPageStatus] = useState<PageStatus>("loading");
  const [pageError, setPageError] = useState<string | null>(null);
  const [sourceRecordings, setSourceRecordings] = useState<SourceRecording[]>([]);
  const [selectedRecordingIds, setSelectedRecordingIds] = useState<string[]>([]);
  const [referenceRuns, setReferenceRuns] = useState<ReferenceRun[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [selectedRun, setSelectedRun] = useState<ReferenceRun | null>(null);
  const [candidates, setCandidates] = useState<ReferenceCandidate[]>([]);
  const [candidateError, setCandidateError] = useState<string | null>(null);
  const [isCreatingRun, setIsCreatingRun] = useState(false);
  const [promotingCandidateId, setPromotingCandidateId] = useState<string | null>(null);
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
    setCandidateError(null);

    if (!projectId) {
      setSourceRecordings([]);
      setSelectedRecordingIds([]);
      setReferenceRuns([]);
      setSelectedRunId(null);
      setSelectedRun(null);
      setCandidates([]);
      setReferenceAssets([]);
      setSelectedAssetId(null);
      setSelectedAsset(null);
      setPageStatus("ready");
      return;
    }

    try {
      const [nextRecordings, nextRuns, nextAssets] = await Promise.all([
        fetchProjectSourceRecordings(projectId),
        fetchProjectReferenceRuns(projectId),
        fetchProjectReferenceAssets(projectId),
      ]);

      if (latestLoadRequestRef.current !== requestId) {
        return;
      }

      const assetIdFromLocation = readSelectedAssetIdFromLocation();
      setSourceRecordings(nextRecordings);
      setSelectedRecordingIds((current) => {
        const filtered = current.filter((recordingId) => nextRecordings.some((recording) => recording.id === recordingId));
        if (filtered.length > 0) {
          return filtered;
        }
        if (nextRecordings.length === 1) {
          return [nextRecordings[0].id];
        }
        return [];
      });
      setReferenceRuns(nextRuns);
      setSelectedRunId((current) =>
        current && nextRuns.some((run) => run.id === current) ? current : (nextRuns[0]?.id ?? null),
      );
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
      setReferenceRuns([]);
      setSelectedRunId(null);
      setSelectedRun(null);
      setCandidates([]);
      setReferenceAssets([]);
      setSelectedAssetId(null);
      setSelectedAsset(null);
      setPageStatus("error");
      setPageError(
        getReferenceErrorMessage(error, "The reference workstation failed to load. Check the backend and try again."),
      );
    }
  }

  async function refreshReferenceAssets(projectId: string, preferredAssetId?: string | null) {
    const nextAssets = await fetchProjectReferenceAssets(projectId);
    setReferenceAssets(nextAssets);
    setSelectedAssetId((current) => {
      if (preferredAssetId && nextAssets.some((asset) => asset.id === preferredAssetId)) {
        return preferredAssetId;
      }
      return current && nextAssets.some((asset) => asset.id === current) ? current : (nextAssets[0]?.id ?? null);
    });
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
      replaceSelectedAssetInLocation(null);
      return;
    }

    replaceSelectedAssetInLocation(selectedAssetId);
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
        setPageError(getReferenceErrorMessage(error, "The selected reference could not be loaded."));
      }
    })();
  }, [selectedAssetId]);

  useEffect(() => {
    if (!selectedRunId) {
      setSelectedRun(null);
      setCandidates([]);
      setCandidateError(null);
      return;
    }

    let cancelled = false;
    let intervalId: number | null = null;

    async function loadRunOnce(runId: string) {
      try {
        const run = await fetchReferenceRun(runId);
        if (cancelled) {
          return;
        }
        setSelectedRun(run);
        setReferenceRuns((current) =>
          current.some((item) => item.id === run.id)
            ? current.map((item) => (item.id === run.id ? run : item))
            : [run, ...current],
        );

        if (run.status === "completed") {
          const nextCandidates = await fetchReferenceRunCandidates(run.id, { limit: 100 });
          if (cancelled) {
            return;
          }
          setCandidates(nextCandidates);
          setCandidateError(null);
          if (intervalId !== null) {
            window.clearInterval(intervalId);
            intervalId = null;
          }
        } else {
          setCandidates([]);
          if (run.status === "failed") {
            setCandidateError(run.error_message || "The run failed.");
            if (intervalId !== null) {
              window.clearInterval(intervalId);
              intervalId = null;
            }
          }
        }
      } catch (error) {
        if (cancelled) {
          return;
        }
        setSelectedRun(null);
        setCandidates([]);
        setCandidateError(getReferenceErrorMessage(error, "The selected run could not be loaded."));
        if (intervalId !== null) {
          window.clearInterval(intervalId);
          intervalId = null;
        }
      }
    }

    void loadRunOnce(selectedRunId);
    intervalId = window.setInterval(() => {
      void loadRunOnce(selectedRunId);
    }, 1500);

    return () => {
      cancelled = true;
      if (intervalId !== null) {
        window.clearInterval(intervalId);
      }
    };
  }, [selectedRunId]);

  const promotedCandidatesById = useMemo(() => {
    const promoted = new Map<string, ReferenceAssetSummary>();
    for (const asset of referenceAssets) {
      if (asset.created_from_candidate_id) {
        promoted.set(asset.created_from_candidate_id, asset);
      }
    }
    return promoted;
  }, [referenceAssets]);

  async function handleCreateRun() {
    if (!activeProject?.id || selectedRecordingIds.length === 0) {
      return;
    }
    setIsCreatingRun(true);
    setCandidateError(null);
    try {
      const run = await createReferenceRun(activeProject.id, {
        recording_ids: selectedRecordingIds,
        mode: "both",
        candidate_count_cap: 60,
      });
      setReferenceRuns((current) => [run, ...current.filter((item) => item.id !== run.id)]);
      setSelectedRunId(run.id);
    } catch (error) {
      setCandidateError(getReferenceErrorMessage(error, "The candidate run could not be started."));
    } finally {
      setIsCreatingRun(false);
    }
  }

  async function handlePromoteCandidate(candidate: ReferenceCandidate) {
    if (!activeProject?.id) {
      return;
    }
    setPromotingCandidateId(candidate.candidate_id);
    setCandidateError(null);
    try {
      const asset = await promoteReferenceCandidate({
        run_id: candidate.run_id,
        candidate_id: candidate.candidate_id,
      });
      await refreshReferenceAssets(activeProject.id, asset.id);
    } catch (error) {
      setCandidateError(
        getReferenceErrorMessage(error, "The candidate could not be promoted to the reference library."),
      );
    } finally {
      setPromotingCandidateId(null);
    }
  }

  function toggleRecording(recordingId: string) {
    setSelectedRecordingIds((current) =>
      current.includes(recordingId)
        ? current.filter((item) => item !== recordingId)
        : [...current, recordingId],
    );
  }

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
          <ReferenceRunSidebar
            pageStatus={pageStatus}
            pageError={pageError}
            sourceRecordings={sourceRecordings}
            selectedRecordingIds={selectedRecordingIds}
            referenceRuns={referenceRuns}
            selectedRunId={selectedRunId}
            isCreatingRun={isCreatingRun}
            onRetryLoad={() => void loadPage(activeProject?.id)}
            onToggleRecording={toggleRecording}
            onCreateRun={() => void handleCreateRun()}
            onSelectRun={setSelectedRunId}
          />

          <ReferenceCandidatePane
            selectedRun={selectedRun}
            candidates={candidates}
            candidateError={candidateError}
            promotingCandidateId={promotingCandidateId}
            promotedCandidatesById={promotedCandidatesById}
            onPromoteCandidate={(candidate) => void handlePromoteCandidate(candidate)}
            onOpenExistingAsset={setSelectedAssetId}
          />

          <ReferenceLibraryPane
            pageStatus={pageStatus}
            referenceAssets={referenceAssets}
            selectedAssetId={selectedAssetId}
            selectedAsset={selectedAsset}
            onSelectAsset={setSelectedAssetId}
          />
        </div>
      )}
    </section>
  );
}
