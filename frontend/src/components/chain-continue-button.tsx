"use client";

import { SubmitButton } from "@midday/ui/submit-button";
import { useRouter, useSearchParams } from "next/navigation";
import { useState } from "react";
import type { ReactNode } from "react";
import { demoEnabled, withDemo } from "@/lib/demo";

type Props = {
  href: string;
  label?: ReactNode;
  icon?: ReactNode;
};

export function ChainContinueButton({
  href,
  label = "Continue",
  icon,
}: Props) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [isLoading, setLoading] = useState(false);

  const handleContinue = () => {
    setLoading(true);
    router.push(withDemo(href, demoEnabled(searchParams)));
  };

  return (
    <SubmitButton
      type="button"
      onClick={handleContinue}
      className="bg-primary px-6 py-4 text-secondary font-medium flex space-x-2 h-[40px] w-full"
      isSubmitting={isLoading}
    >
      <div className="flex items-center justify-center gap-2">
        {icon}
        <span>{label}</span>
      </div>
    </SubmitButton>
  );
}
