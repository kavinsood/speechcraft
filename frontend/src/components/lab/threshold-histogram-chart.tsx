"use client";

import { useCallback, useMemo, useRef, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { HistogramBin } from "./qc-logic";

type ThresholdHistogramChartProps = {
  title: string;
  subtitle: string;
  bins: HistogramBin[];
  threshold: number;
  onThresholdChange: (value: number) => void;
  unscoredCount: number;
  acceptedCount: number;
  acceptedDurationSec: number;
  height?: number;
};

function formatDuration(seconds: number): string {
  const mins = Math.floor(seconds / 60);
  const secs = Math.round(seconds % 60);
  if (mins <= 0) return `${secs}s`;
  return `${mins}m ${secs}s`;
}

type TooltipPayloadItem = {
  payload?: HistogramBin;
};

function HistogramTooltip({
  active,
  payload,
}: {
  active?: boolean;
  payload?: TooltipPayloadItem[];
}) {
  if (!active || !payload?.length) return null;
  const bin = payload[0]?.payload;
  if (!bin) return null;
  return (
    <div className="border border-border bg-background px-2.5 py-1.5 text-xs shadow-sm">
      <p className="text-[#878787]">
        {Math.round(bin.start)}–{Math.round(bin.end)}
      </p>
      <p className="font-medium tabular-nums">
        {bin.count} clip{bin.count === 1 ? "" : "s"}
      </p>
    </div>
  );
}

export function ThresholdHistogramChart({
  title,
  subtitle,
  bins,
  threshold,
  onThresholdChange,
  unscoredCount,
  acceptedCount,
  acceptedDurationSec,
  height = 240,
}: ThresholdHistogramChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [dragging, setDragging] = useState(false);

  // Shared with the <BarChart margin> below: the plot area is the container
  // minus these margins on every side. Keeping this as one constant means the
  // drag math and recharts' own layout can never drift apart.
  const PLOT_MARGIN = { top: 6, right: 8, bottom: 0, left: 8 };

  const data = useMemo(
    () => bins.map((bin) => ({ ...bin, kept: bin.center >= threshold })),
    [bins, threshold],
  );

  const maxCount = useMemo(() => Math.max(1, ...bins.map((b) => b.count)), [bins]);

  const scoreFromClientX = useCallback((clientX: number): number => {
    const el = containerRef.current;
    if (!el) return threshold;
    const rect = el.getBoundingClientRect();
    const plotLeft = rect.left + PLOT_MARGIN.left;
    const plotWidth = rect.width - PLOT_MARGIN.left - PLOT_MARGIN.right;
    if (plotWidth <= 0) return threshold;
    const ratio = (clientX - plotLeft) / plotWidth;
    const clamped = Math.max(0, Math.min(1, ratio));
    return Math.round(clamped * 100);
  }, [threshold, PLOT_MARGIN.left, PLOT_MARGIN.right]);

  const handlePointerDown = useCallback(
    (event: React.PointerEvent<HTMLDivElement>) => {
      event.preventDefault();
      setDragging(true);
      onThresholdChange(scoreFromClientX(event.clientX));
      const onMove = (moveEvent: PointerEvent) => {
        onThresholdChange(scoreFromClientX(moveEvent.clientX));
      };
      const onUp = () => {
        setDragging(false);
        window.removeEventListener("pointermove", onMove);
        window.removeEventListener("pointerup", onUp);
      };
      window.addEventListener("pointermove", onMove);
      window.addEventListener("pointerup", onUp);
    },
    [onThresholdChange, scoreFromClientX],
  );

  const handleKeyDown = useCallback(
    (event: React.KeyboardEvent<HTMLDivElement>) => {
      if (event.key === "ArrowLeft") {
        event.preventDefault();
        onThresholdChange(Math.max(0, threshold - 1));
      } else if (event.key === "ArrowRight") {
        event.preventDefault();
        onThresholdChange(Math.min(100, threshold + 1));
      }
    },
    [onThresholdChange, threshold],
  );

  return (
    <div className="border border-border p-4">
      <div className="mb-1 flex items-baseline justify-between">
        <h3 className="font-serif text-lg leading-none">{title}</h3>
        <span className="text-xs tabular-nums text-[#878787]">min {threshold}</span>
      </div>
      <p className="mb-4 text-xs text-[#878787]">{subtitle}</p>

      <div
        ref={containerRef}
        className="relative touch-none select-none"
        style={{ height }}
        onPointerDown={handlePointerDown}
      >
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={PLOT_MARGIN} barCategoryGap={1}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--chart-grid-stroke, #e6e6e6)" vertical={false} />
            <XAxis
              dataKey="center"
              type="number"
              domain={[0, 100]}
              axisLine={false}
              tickLine={false}
              ticks={[0, 25, 50, 75, 100]}
              tick={{ fill: "#878787", fontSize: 10 }}
            />
            <YAxis hide domain={[0, maxCount]} />
            <Tooltip content={<HistogramTooltip />} cursor={false} />
            <Bar dataKey="count" isAnimationActive={false}>
              {data.map((entry) => (
                <Cell
                  key={entry.start}
                  fill={entry.kept ? "var(--chart-bar-fill)" : "var(--chart-bar-fill-secondary)"}
                />
              ))}
            </Bar>
            <ReferenceLine
              x={threshold}
              stroke="currentColor"
              strokeWidth={1.5}
              zIndex={500}
              className="text-foreground"
            />
          </BarChart>
        </ResponsiveContainer>

        {/* Draggable handle grip, positioned over the reference line. Uses the
            same PLOT_MARGIN as the chart above so it tracks the real bar
            positions instead of the raw container width. */}
        <div
          role="slider"
          aria-label={`${title} minimum threshold`}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuenow={threshold}
          tabIndex={0}
          onKeyDown={handleKeyDown}
          className="absolute top-0 flex h-full w-4 -translate-x-1/2 cursor-ew-resize items-start justify-center outline-none"
          style={{
            left: `calc(${PLOT_MARGIN.left}px + (100% - ${PLOT_MARGIN.left + PLOT_MARGIN.right}px) * ${threshold / 100})`,
          }}
        >
          <div
            className={`mt-[-4px] h-2.5 w-2.5 rotate-45 border border-foreground bg-background transition-transform ${
              dragging ? "scale-125" : ""
            }`}
          />
        </div>
      </div>

      <div className="mt-3 flex items-center justify-between border-t border-border pt-3 text-xs">
        <span className="text-[#878787]">
          {acceptedCount} clip{acceptedCount === 1 ? "" : "s"} kept ·{" "}
          {formatDuration(acceptedDurationSec)}
        </span>
        {unscoredCount > 0 && (
          <span className="text-[#878787]">{unscoredCount} unscored</span>
        )}
      </div>
    </div>
  );
}
