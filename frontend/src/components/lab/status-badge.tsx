import { Badge } from "@midday/ui/badge";
import { cn } from "@midday/ui/cn";
import { type ReviewStatus, STATUS_LABELS } from "./lab-data";

// Live human review status as a solid, semantically-colored Badge — this is
// the authoritative truth (distinct from the muted/outline machine-bucket
// badge used in the inspector).
const STATUS_CLASSES: Record<ReviewStatus, string> = {
  unresolved:
    "border-transparent bg-[#e8dcae] text-[#5c5326] dark:bg-[#33301c] dark:text-[#d9c441] hover:bg-[#e8dcae]",
  accepted:
    "border-transparent bg-emerald-100 text-emerald-800 dark:bg-emerald-950 dark:text-emerald-300 hover:bg-emerald-100",
  rejected:
    "border-transparent bg-red-100 text-red-800 dark:bg-red-950 dark:text-red-300 hover:bg-red-100",
  quarantined:
    "border-transparent bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300 hover:bg-amber-100",
};

export function StatusBadge({
  status,
  className,
}: {
  status: ReviewStatus;
  className?: string;
}) {
  return (
    <Badge className={cn("font-normal", STATUS_CLASSES[status], className)}>
      {STATUS_LABELS[status]}
    </Badge>
  );
}
