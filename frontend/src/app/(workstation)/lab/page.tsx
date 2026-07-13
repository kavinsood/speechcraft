import type { Metadata } from "next";
import { Suspense } from "react";
import { LabWorkstation } from "@/components/lab/lab-workstation";

export const metadata: Metadata = {
  title: "Clip Lab | Speechcraft",
};

// Inherently dynamic: reads URL search params and fetches live data from the
// FastAPI backend. No value in static prerendering a local workstation page.
export const dynamic = "force-dynamic";

export default function LabPage() {
  return (
    <Suspense fallback={null}>
      <LabWorkstation />
    </Suspense>
  );
}
