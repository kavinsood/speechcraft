import type { Metadata } from "next";
import { IngestDropzone } from "@/components/ingest-dropzone";

export const metadata: Metadata = {
  title: "Login | Midday",
};

export default function Page() {
  return <IngestDropzone />;
}
