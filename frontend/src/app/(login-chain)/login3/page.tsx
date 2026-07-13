import type { Metadata } from "next";
import { ModelDownload } from "@/components/model-download";

export const metadata: Metadata = {
  title: "Login | Midday",
};

export default function Page() {
  return <ModelDownload />;
}
