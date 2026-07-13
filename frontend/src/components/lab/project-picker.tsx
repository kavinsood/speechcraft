"use client";

import { Avatar, AvatarFallback } from "@midday/ui/avatar";
import { cn } from "@midday/ui/cn";
import { Icons } from "@midday/ui/icons";
import { Popover, PopoverContent, PopoverTrigger } from "@midday/ui/popover";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@midday/ui/tooltip";
import { useQuery } from "@tanstack/react-query";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { fetchProjects } from "./speechcraft-api";

export function ProjectPicker() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const { data: projects = [] } = useQuery({
    queryKey: ["sc-projects"],
    queryFn: fetchProjects,
    staleTime: 60_000,
  });

  const paramId = searchParams.get("project");
  const activeProject =
    projects.find((p) => p.id === paramId) ?? projects[0] ?? null;

  const selectProject = (id: string) => {
    const params = new URLSearchParams(searchParams.toString());
    params.set("project", id);
    router.replace(`${pathname}?${params.toString()}`);
  };

  const initials = (activeProject?.name ?? "··").slice(0, 2).toUpperCase();

  return (
    <TooltipProvider delayDuration={50}>
      <Popover>
        <Tooltip>
          <TooltipTrigger asChild>
            <PopoverTrigger asChild>
              <button
                type="button"
                aria-label="Switch project"
                className="flex items-center justify-center"
              >
                <Avatar className="h-8 w-8 rounded-none border border-border">
                  <AvatarFallback className="rounded-none text-xs">
                    {initials}
                  </AvatarFallback>
                </Avatar>
              </button>
            </PopoverTrigger>
          </TooltipTrigger>
          <TooltipContent side="bottom" sideOffset={8}>
            <p className="text-xs">{activeProject?.name ?? "No project"}</p>
          </TooltipContent>
        </Tooltip>

        <PopoverContent side="bottom" align="end" className="w-56 p-1">
          <p className="px-2 py-1.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
            Project
          </p>
          {projects.map((project) => {
            const isActive = project.id === activeProject?.id;
            return (
              <button
                key={project.id}
                type="button"
                onClick={() => selectProject(project.id)}
                className={cn(
                  "flex w-full items-center justify-between px-2 py-1.5 text-sm transition-colors hover:bg-secondary",
                  isActive && "text-foreground",
                )}
              >
                <span className="flex items-center gap-2">
                  <Avatar className="h-5 w-5 rounded-none border border-border">
                    <AvatarFallback className="rounded-none text-[9px]">
                      {project.name.slice(0, 2).toUpperCase()}
                    </AvatarFallback>
                  </Avatar>
                  {project.name}
                </span>
                {isActive ? <Icons.Check className="size-4" /> : null}
              </button>
            );
          })}
        </PopoverContent>
      </Popover>
    </TooltipProvider>
  );
}
