"use client";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@midday/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@midday/ui/table";
import { useMemo, useState } from "react";
import {
  bestRejected,
  riskiestKept,
  type KeptSort,
  type ManualOverride,
  type QcClip,
  type RejectedSort,
} from "./qc-logic";

function formatScore(score: number | null): string {
  return score === null ? "—" : score.toFixed(1);
}

function formatMargin(margin: number): string {
  if (!Number.isFinite(margin)) return "—";
  const sign = margin > 0 ? "+" : "";
  return `${sign}${margin.toFixed(1)}`;
}

function truncate(text: string, max = 64): string {
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

type BoundaryTablesProps = {
  clips: QcClip[];
  transcriptThreshold: number;
  speakerThreshold: number;
  overrides?: Record<string, ManualOverride | null | undefined>;
  limit?: number;
};

export function RiskiestKeptTable({
  clips,
  transcriptThreshold,
  speakerThreshold,
  overrides = {},
  limit = 10,
}: BoundaryTablesProps) {
  const [sort, setSort] = useState<KeptSort>("risk");

  const rows = useMemo(
    () =>
      riskiestKept(clips, transcriptThreshold, speakerThreshold, sort, overrides).slice(0, limit),
    [clips, transcriptThreshold, speakerThreshold, sort, overrides, limit],
  );

  return (
    <div className="border border-border">
      <div className="flex items-center justify-between border-b border-border p-4">
        <div>
          <h3 className="font-serif text-lg leading-none">Riskiest kept</h3>
          <p className="mt-1 text-xs text-[#878787]">
            Accepted clips closest to failing — the ones worth a human ear.
          </p>
        </div>
        <Select value={sort} onValueChange={(v) => setSort(v as KeptSort)}>
          <SelectTrigger className="h-8 w-[160px] text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="risk">Sort by risk</SelectItem>
            <SelectItem value="transcript">Closest — transcript</SelectItem>
            <SelectItem value="speaker">Closest — speaker</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {rows.length === 0 ? (
        <p className="p-4 text-sm text-[#878787]">No accepted clips at these thresholds.</p>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Clip</TableHead>
              <TableHead className="text-right">Transcript</TableHead>
              <TableHead className="text-right">Speaker</TableHead>
              <TableHead className="text-right">Risk margin</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map(({ clip, transcriptMargin, speakerMargin, riskMargin }) => (
              <TableRow key={clip.clipId}>
                <TableCell className="max-w-0">
                  <span className="block truncate font-mono text-xs">{clip.clipId}</span>
                  {clip.trainingText && (
                    <span className="block truncate text-xs text-[#878787]">
                      {truncate(clip.trainingText)}
                    </span>
                  )}
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {formatScore(clip.transcriptMatch)}
                  <span className="ml-1 text-[#878787]">({formatMargin(transcriptMargin)})</span>
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {formatScore(clip.speakerCheck)}
                  <span className="ml-1 text-[#878787]">({formatMargin(speakerMargin)})</span>
                </TableCell>
                <TableCell className="text-right tabular-nums">{formatMargin(riskMargin)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </div>
  );
}

export function BestRejectedTable({
  clips,
  transcriptThreshold,
  speakerThreshold,
  overrides = {},
  limit = 10,
}: BoundaryTablesProps) {
  const [sort, setSort] = useState<RejectedSort>("closest");

  const rows = useMemo(
    () =>
      bestRejected(clips, transcriptThreshold, speakerThreshold, sort, overrides).slice(0, limit),
    [clips, transcriptThreshold, speakerThreshold, sort, overrides, limit],
  );

  return (
    <div className="border border-border">
      <div className="flex items-center justify-between border-b border-border p-4">
        <div>
          <h3 className="font-serif text-lg leading-none">Best rejected</h3>
          <p className="mt-1 text-xs text-[#878787]">
            Rejected clips closest to passing — recoverable if thresholds relax.
          </p>
        </div>
        <Select value={sort} onValueChange={(v) => setSort(v as RejectedSort)}>
          <SelectTrigger className="h-8 w-[160px] text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="closest">Closest to passing</SelectItem>
            <SelectItem value="transcript_only">Transcript-only fail</SelectItem>
            <SelectItem value="speaker_only">Speaker-only fail</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {rows.length === 0 ? (
        <p className="p-4 text-sm text-[#878787]">No rejected clips match this filter.</p>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Clip</TableHead>
              <TableHead className="text-right">Transcript</TableHead>
              <TableHead className="text-right">Speaker</TableHead>
              <TableHead className="text-right">Recovery gap</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map(({ clip, transcriptGap, speakerGap, recoveryGap }) => (
              <TableRow key={clip.clipId}>
                <TableCell className="max-w-0">
                  <span className="block truncate font-mono text-xs">{clip.clipId}</span>
                  {clip.trainingText && (
                    <span className="block truncate text-xs text-[#878787]">
                      {truncate(clip.trainingText)}
                    </span>
                  )}
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {formatScore(clip.transcriptMatch)}
                  {transcriptGap > 0 && (
                    <span className="ml-1 text-[#878787]">(-{transcriptGap.toFixed(1)})</span>
                  )}
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {formatScore(clip.speakerCheck)}
                  {speakerGap > 0 && (
                    <span className="ml-1 text-[#878787]">(-{speakerGap.toFixed(1)})</span>
                  )}
                </TableCell>
                <TableCell className="text-right tabular-nums">{recoveryGap.toFixed(1)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </div>
  );
}
