"use client";

import { SubmitButton } from "@midday/ui/submit-button";
import { useRouter } from "next/navigation";
import { useState } from "react";

// End of the setup wizard: hand off into the Lab workstation. No auth
// return_to concept anymore (local single-user tool).
export function LoginContinueButton() {
  const router = useRouter();
  const [isLoading, setLoading] = useState(false);

  const handleContinue = () => {
    setLoading(true);
    router.push("/lab");
  };

  return (
    <SubmitButton
      type="button"
      onClick={handleContinue}
      className="bg-primary px-6 py-4 text-secondary font-medium flex space-x-2 h-[40px] w-full"
      isSubmitting={isLoading}
    >
      Continue
    </SubmitButton>
  );
}
