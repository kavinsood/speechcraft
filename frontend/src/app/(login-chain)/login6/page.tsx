import type { Metadata } from "next";
import { LoginContinueButton } from "@/components/login-continue-button";

export const metadata: Metadata = {
  title: "Login | Midday",
};

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
        <LoginContinueButton />
      </div>
    </>
  );
}
