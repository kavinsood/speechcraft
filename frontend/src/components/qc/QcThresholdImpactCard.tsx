import { useEffect, useMemo, useRef, useState } from "react";
import type { CurvePoint } from "../../qc/qcLogic";

type QcThresholdImpactCardProps = {
  label: string;
  threshold: number;
  points: CurvePoint[];
  accentClassName: string;
  emptyLabel: string;
  explanation: string;
  onDraftChange: (value: number) => void;
  onCommit: () => void;
};

const VIEWBOX_WIDTH = 320;
const VIEWBOX_HEIGHT = 180;
const RANGE_THUMB_WIDTH_PX = 14;
const X_TICKS = [0, 25, 50, 75, 100];
const Y_MAX_PADDING_MULTIPLIER = 1.03;
const HOVER_LINE_TOLERANCE = 16;

type HoverState = {
  threshold: number;
  x: number;
  y: number;
  leftPercent: number;
  topPercent: number;
};

function useElementWidth<T extends HTMLElement>() {
  const ref = useRef<T | null>(null);
  const [width, setWidth] = useState(0);

  useEffect(() => {
    const element = ref.current;
    if (!element) {
      return undefined;
    }

    const update = () => {
      setWidth(element.getBoundingClientRect().width);
    };
    update();

    if (typeof ResizeObserver === "undefined") {
      window.addEventListener("resize", update);
      return () => window.removeEventListener("resize", update);
    }

    const observer = new ResizeObserver(update);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  return { ref, width };
}

function thresholdToSvgX(threshold: number, renderedWidthPx: number): number {
  const clampedWidth = renderedWidthPx > 0 ? renderedWidthPx : VIEWBOX_WIDTH;
  if (clampedWidth <= RANGE_THUMB_WIDTH_PX) {
    return (threshold / 100) * VIEWBOX_WIDTH;
  }

  const pct = threshold / 100;
  const thumbRadius = RANGE_THUMB_WIDTH_PX / 2;
  const xPx = thumbRadius + pct * (clampedWidth - RANGE_THUMB_WIDTH_PX);
  return (xPx / clampedWidth) * VIEWBOX_WIDTH;
}

function pathFromPoints(
  points: CurvePoint[],
  renderedWidthPx: number,
  yMinSec: number,
  yMaxSec: number,
): string {
  if (points.length === 0) {
    return "";
  }

  if (yMaxSec <= yMinSec) {
    return "";
  }

  return points
    .map((point, index) => {
      const x = thresholdToSvgX(point.threshold, renderedWidthPx || VIEWBOX_WIDTH);
      const y = durationToSvgY(point.acceptedDurationSec, yMinSec, yMaxSec);
      return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(" ");
}

function durationToSvgY(durationSec: number, minDurationSec: number, maxDurationSec: number): number {
  if (!Number.isFinite(maxDurationSec) || maxDurationSec <= minDurationSec) {
    return VIEWBOX_HEIGHT;
  }

  const clampedDuration = Math.max(minDurationSec, Math.min(maxDurationSec, durationSec));
  return VIEWBOX_HEIGHT - ((clampedDuration - minDurationSec) / (maxDurationSec - minDurationSec)) * VIEWBOX_HEIGHT;
}

function durationDomainForPoints(points: CurvePoint[]): { min: number; max: number } {
  const values = points
    .map((point) => point.acceptedDurationSec)
    .filter((value) => Number.isFinite(value));

  if (values.length === 0) {
    return { min: 0, max: 60 };
  }

  const min = Math.min(...values);
  const max = Math.max(...values);

  if (max <= min) {
    const pad = Math.max(1, max * 0.05);
    return {
      min: Math.max(0, min - pad),
      max: max + pad,
    };
  }

  const pad = Math.max(1, (max - min) * 0.08);
  return {
    min: Math.max(0, min - pad),
    max: max + pad,
  };
}

function formatDurationCompact(totalSeconds: number): string {
  if (!Number.isFinite(totalSeconds) || totalSeconds <= 0) {
    return "0s";
  }

  if (totalSeconds >= 3600) {
    return `${(totalSeconds / 3600).toFixed(1)}h`;
  }
  if (totalSeconds >= 60) {
    return `${(totalSeconds / 60).toFixed(1)}m`;
  }
  return `${Math.round(totalSeconds)}s`;
}

function formatDurationHms(totalSeconds: number): string {
  const roundedSeconds = Math.max(0, Math.round(totalSeconds));
  const hours = Math.floor(roundedSeconds / 3600);
  const minutes = Math.floor((roundedSeconds % 3600) / 60);
  const seconds = roundedSeconds % 60;

  if (hours > 0) {
    return `${hours}h ${minutes}m ${seconds}s`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  }
  return `${seconds}s`;
}

function thresholdFromPointer(clientX: number, element: HTMLDivElement | null, fallbackThreshold: number): number {
  if (!element) {
    return fallbackThreshold;
  }

  const rect = element.getBoundingClientRect();
  const thumbRadius = RANGE_THUMB_WIDTH_PX / 2;
  const rawX = clientX - rect.left;
  const pct = (rawX - thumbRadius) / Math.max(1, rect.width - RANGE_THUMB_WIDTH_PX);
  return Math.max(0, Math.min(100, Math.round(pct * 100)));
}

function pointerToSvgY(clientY: number, frame: HTMLDivElement | null): number | null {
  if (!frame) {
    return null;
  }

  const rect = frame.getBoundingClientRect();
  if (rect.height <= 0) {
    return null;
  }

  const rawY = clientY - rect.top;
  const clampedY = Math.max(0, Math.min(rect.height, rawY));
  return (clampedY / rect.height) * VIEWBOX_HEIGHT;
}

export default function QcThresholdImpactCard({
  label,
  threshold,
  points,
  accentClassName,
  emptyLabel,
  explanation,
  onDraftChange,
  onCommit,
}: QcThresholdImpactCardProps) {
  const maxDurationSec = useMemo(
    () => Math.max(...points.map((point) => point.acceptedDurationSec), 0),
    [points],
  );
  const yDomain = useMemo(() => durationDomainForPoints(points), [points]);
  const { ref: plotAreaRef, width: plotWidth } = useElementWidth<HTMLDivElement>();
  const path = useMemo(
    () => pathFromPoints(points, plotWidth, yDomain.min, yDomain.max),
    [points, plotWidth, yDomain.min, yDomain.max],
  );
  const hasData = path.length > 0;
  const thresholdX = thresholdToSvgX(threshold, plotWidth || VIEWBOX_WIDTH);
  const curveFrameRef = useRef<HTMLDivElement | null>(null);
  const rafRef = useRef<number | null>(null);
  const [hover, setHover] = useState<HoverState | null>(null);

  useEffect(() => {
    return () => {
      if (rafRef.current !== null) {
        window.cancelAnimationFrame(rafRef.current);
      }
    };
  }, []);

  const handlePointerMove = (event: React.PointerEvent<SVGRectElement>) => {
    const clientX = event.clientX;
    const clientY = event.clientY;
    if (rafRef.current !== null) {
      window.cancelAnimationFrame(rafRef.current);
    }

    rafRef.current = window.requestAnimationFrame(() => {
      const nextThreshold = thresholdFromPointer(clientX, plotAreaRef.current, threshold);
      const point = points[nextThreshold];
      const frameRect = curveFrameRef.current?.getBoundingClientRect();

      if (!point || !frameRect || frameRect.width <= 0) {
        setHover(null);
        return;
      }

      const rawX = clientX - frameRect.left;
      const clampedX = Math.max(0, Math.min(frameRect.width, rawX));
      const visualX = (clampedX / frameRect.width) * VIEWBOX_WIDTH;
      const y = durationToSvgY(point.acceptedDurationSec, yDomain.min, yDomain.max);
      const pointerY = pointerToSvgY(clientY, curveFrameRef.current);

      if (pointerY === null || Math.abs(pointerY - y) > HOVER_LINE_TOLERANCE) {
        setHover(null);
        return;
      }

      setHover({
        threshold: nextThreshold,
        x: visualX,
        y,
        leftPercent: (visualX / VIEWBOX_WIDTH) * 100,
        topPercent: (y / VIEWBOX_HEIGHT) * 100,
      });
    });
  };

  const hoverPoint = hover ? points[hover.threshold] ?? null : null;

  return (
    <article className={`qc-threshold-impact-card ${accentClassName}`}>
      <div className="qc-threshold-impact-head">
        <div>
          <h4>{label}</h4>
          <p>Reject below {threshold}</p>
        </div>
        <div className="qc-threshold-impact-badge">Threshold {threshold}</div>
      </div>

      <div className="qc-threshold-impact-visual">
        <div className="qc-threshold-impact-meta">
          <span>Accepted duration</span>
          <span>max {formatDurationCompact(maxDurationSec)}</span>
        </div>

        <div ref={plotAreaRef} className="qc-plot-area">
          <input
            className="qc-threshold-slider"
            type="range"
            min="0"
            max="100"
            step="1"
            value={threshold}
            onChange={(event) => onDraftChange(Number(event.target.value))}
            onMouseUp={onCommit}
            onTouchEnd={onCommit}
            onKeyUp={onCommit}
            onBlur={onCommit}
          />

          <div ref={curveFrameRef} className="qc-curve-frame">
            {hover && hoverPoint ? (
              <div
                className="qc-curve-tooltip"
                style={{ left: `${hover.leftPercent}%`, top: `${hover.topPercent}%` }}
              >
                <strong>Threshold = {hover.threshold}</strong>
                <span>Duration = {formatDurationHms(hoverPoint.acceptedDurationSec)}</span>
              </div>
            ) : null}

            <svg
              aria-label={`${label} threshold impact curve`}
              className="qc-curve-svg"
              viewBox={`0 0 ${VIEWBOX_WIDTH} ${VIEWBOX_HEIGHT}`}
              preserveAspectRatio="none"
              role="img"
            >
              {X_TICKS.map((tick) => {
                const x = thresholdToSvgX(tick, plotWidth || VIEWBOX_WIDTH);
                return (
                  <line
                    key={`tick-${tick}`}
                    className="qc-curve-grid qc-curve-grid-vertical"
                    x1={x}
                    y1="0"
                    x2={x}
                    y2={VIEWBOX_HEIGHT}
                  />
                );
              })}
              <line className="qc-curve-grid" x1="0" y1={VIEWBOX_HEIGHT} x2={VIEWBOX_WIDTH} y2={VIEWBOX_HEIGHT} />
              <line
                className="qc-curve-grid"
                x1="0"
                y1={VIEWBOX_HEIGHT / 2}
                x2={VIEWBOX_WIDTH}
                y2={VIEWBOX_HEIGHT / 2}
              />
              <line className="qc-curve-threshold" x1={thresholdX} y1="0" x2={thresholdX} y2={VIEWBOX_HEIGHT} />
              {hasData ? <path className="qc-curve-path" d={path} /> : null}
              {hover ? (
                <>
                  <line className="qc-curve-hover-line" x1={hover.x} y1="0" x2={hover.x} y2={VIEWBOX_HEIGHT} />
                  <circle className="qc-curve-hover-dot" cx={hover.x} cy={hover.y} r="4" />
                </>
              ) : null}
              <rect
                className="qc-curve-hit-area"
                x="0"
                y="0"
                width={VIEWBOX_WIDTH}
                height={VIEWBOX_HEIGHT}
                onPointerMove={handlePointerMove}
                onPointerLeave={() => setHover(null)}
              />
            </svg>
          </div>

          <div className="qc-x-axis">
            {X_TICKS.map((tick) => (
              <span key={tick}>{tick}</span>
            ))}
          </div>
        </div>

        {!hasData ? <p className="qc-curve-empty">{emptyLabel}</p> : null}
      </div>

      <p className="qc-threshold-impact-note">{explanation}</p>
    </article>
  );
}
