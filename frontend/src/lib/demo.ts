// Demo / UI-review mode. Lets the whole flow (wizard + Lab) be exercised on
// stub data with no ML backend, for reviewing UX/parity without models. Real
// functional wiring is verified by code (types/build/tests); this is purely a
// review affordance. Enabled by NEXT_PUBLIC_SC_DEMO=1 (global) or ?demo=1.

export function demoEnabled(params?: URLSearchParams | null): boolean {
  if (typeof process !== "undefined" && process.env.NEXT_PUBLIC_SC_DEMO === "1") {
    return true;
  }
  return (params?.get("demo") ?? null) === "1";
}

/** Append demo=1 to a URL when demo mode is active, preserving threading. */
export function withDemo(url: string, active: boolean): string {
  if (!active) return url;
  return url + (url.includes("?") ? "&" : "?") + "demo=1";
}
