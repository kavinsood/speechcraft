"use client";

import { Button } from "@midday/ui/button";
import { Icons } from "@midday/ui/icons";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@midday/ui/tooltip";
import Link from "next/link";
import { useState } from "react";
import { ExportDialog } from "./export-dialog";
import { ProjectPicker } from "./project-picker";

export function WorkstationSidebar() {
  const [exportOpen, setExportOpen] = useState(false);

  return (
    <TooltipProvider delayDuration={50}>
      <aside className="fixed left-0 top-0 z-50 flex h-screen w-[70px] flex-col items-center justify-between border-r border-border bg-background pb-4">
        {/* Logo */}
        <div className="flex h-[70px] w-full items-center justify-center border-b border-border">
          <Link href="/" aria-label="Home">
            <Icons.LogoSmall />
          </Link>
        </div>

        {/* Global actions */}
        <div className="flex flex-1 flex-col items-center gap-2 pt-4">
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                type="button"
                variant="ghost"
                size="icon"
                aria-label="Export dataset"
                onClick={() => setExportOpen(true)}
              >
                <Icons.Share className="size-5" />
              </Button>
            </TooltipTrigger>
            <TooltipContent side="right">Export</TooltipContent>
          </Tooltip>

          <Tooltip>
            <TooltipTrigger asChild>
              <Button type="button" variant="ghost" size="icon" aria-label="Settings">
                <Icons.Settings />
              </Button>
            </TooltipTrigger>
            <TooltipContent side="right">Settings</TooltipContent>
          </Tooltip>
        </div>

        {/* Project picker */}
        <ProjectPicker />
      </aside>

      <ExportDialog open={exportOpen} onOpenChange={setExportOpen} />
    </TooltipProvider>
  );
}
