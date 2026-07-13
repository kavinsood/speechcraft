import type { Metadata } from "next";
import { TranscriptionProgress } from "@/components/transcription-progress";

export const metadata: Metadata = {
  title: "Login | Midday",
};

export default function Page() {
  return <TranscriptionProgress />;
}
