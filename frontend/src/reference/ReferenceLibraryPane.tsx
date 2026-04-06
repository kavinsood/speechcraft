import { buildReferenceVariantAudioUrl } from "../api";
import type { ReferenceAssetDetail, ReferenceAssetSummary } from "../types";
import { formatReferenceDuration } from "./reference-helpers";

type ReferenceLibraryPaneProps = {
  pageStatus: "loading" | "ready" | "error";
  referenceAssets: ReferenceAssetSummary[];
  selectedAssetId: string | null;
  selectedAsset: ReferenceAssetDetail | null;
  onSelectAsset: (assetId: string) => void;
};

export default function ReferenceLibraryPane({
  pageStatus,
  referenceAssets,
  selectedAssetId,
  selectedAsset,
  onSelectAsset,
}: ReferenceLibraryPaneProps) {
  const activeVariant = selectedAsset?.active_variant ?? null;
  const activeVariantAudioUrl = activeVariant ? buildReferenceVariantAudioUrl(activeVariant.id) : null;

  return (
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
              onClick={() => onSelectAsset(asset.id)}
            >
              <div className="commit-row">
                <strong>{asset.name}</strong>
                <span>{asset.status}</span>
              </div>
              <p>{asset.transcript_text || "No transcript preview"}</p>
              <span className="commit-time">
                {asset.speaker_name || "speaker n/a"}
                {asset.active_variant
                  ? ` • ${formatReferenceDuration(
                      asset.active_variant.num_samples / Math.max(asset.active_variant.sample_rate, 1),
                    )}`
                  : ""}
              </span>
            </button>
          ))}
        </div>
      ) : null}

      <section className="inspector-block">
        <h3>{selectedAsset?.name ?? "Reference preview"}</h3>
        {!selectedAsset ? (
          <p className="muted-copy">
            Promoted candidates land here immediately and remain usable even if the run artifacts are deleted.
          </p>
        ) : (
          <>
            <p className="muted-copy">
              Saved assets are durable library objects. Candidate cards on the center pane are temporary run output.
            </p>
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
          </>
        )}
      </section>
    </aside>
  );
}
