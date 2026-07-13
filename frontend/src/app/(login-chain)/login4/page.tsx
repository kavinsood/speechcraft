import type { Metadata } from "next";
import { VoiceSelection } from "@/components/voice-selection";

export const metadata: Metadata = {
  title: "Login | Midday",
};

export default function Page() {
  return <VoiceSelection />;
}
