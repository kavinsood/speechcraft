import { ApiError } from "../api";

export function getReferenceErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof ApiError) {
    return error.message;
  }

  if (error instanceof Error && error.message.trim()) {
    return error.message;
  }

  return fallback;
}

export function formatReferenceDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return "0.0s";
  }
  return `${seconds.toFixed(seconds >= 10 ? 1 : 2)}s`;
}

export function readSelectedAssetIdFromLocation(): string | null {
  const assetId = new URLSearchParams(window.location.search).get("asset")?.trim() ?? null;
  return assetId && assetId.length > 0 ? assetId : null;
}

export function replaceSelectedAssetInLocation(assetId: string | null): void {
  const url = new URL(window.location.href);
  if (assetId) {
    url.searchParams.set("asset", assetId);
  } else {
    url.searchParams.delete("asset");
  }
  window.history.replaceState({}, "", url);
}
