import type { Metadata } from "next";
import { ChainContinueButton } from "@/components/chain-continue-button";

export const metadata: Metadata = {
  title: "Speechcraft",
};

// Landing step of the setup wizard. No auth — this is a local single-user
// tool, so there is no Supabase user, no waitlist gate. Just the entry point
// into the ingest -> model -> voice -> transcription chain.
export default function Page() {
  return (
    <>
      <div className="text-center space-y-2">
        <h1 className="text-lg lg:text-xl mb-4 font-serif">
          Train better voice models.
        </h1>
        <p className="font-sans text-sm text-[#878787]">
          The internal tool every voice AI lab built for themselves.
        </p>
      </div>

      <div className="mt-10 lg:mt-12">
        <ChainContinueButton href="/login2" />
      </div>
    </>
  );
}
