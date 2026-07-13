"use client";

import { useEffect, useRef, useState } from "react";
import WaveSurfer from "wavesurfer.js";
import RegionsPlugin from "wavesurfer.js/dist/plugins/regions.esm.js";

type WaveformPaneProps = {
  audioUrl: string;
  selectionStart: number;
  selectionEnd: number;
  onSelectionChange: (start: number, end: number) => void;
  onCursorChange: (time: number) => void;
  onHoverChange?: (time: number | null) => void;
  onReady?: (instance: WaveSurfer | null) => void;
  onPlayingChange?: (isPlaying: boolean) => void;
};

// Adapted from speechcraft's WaveformPane — real wavesurfer + regions, but
// restyled to Midday's monochrome ink palette (was teal) and simplified to
// decode audio directly (no cached-peaks machinery). Keeps the core
// mechanics: click-to-seek, drag-to-select region, hover readout, ctrl+wheel
// zoom, and exposes the instance via onReady so the transport can drive it.
export function WaveformPane({
  audioUrl,
  selectionStart,
  selectionEnd,
  onSelectionChange,
  onCursorChange,
  onHoverChange,
  onReady,
  onPlayingChange,
}: WaveformPaneProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const waveSurferRef = useRef<WaveSurfer | null>(null);
  const regionsRef = useRef<ReturnType<typeof RegionsPlugin.create> | null>(null);
  const zoomRef = useRef(0);
  const pointerDownRef = useRef(false);
  const pointerStartXRef = useRef<number | null>(null);
  const pointerStartTimeRef = useRef<number | null>(null);
  const draggedRef = useRef(false);

  const selectionChangeRef = useRef(onSelectionChange);
  const cursorChangeRef = useRef(onCursorChange);
  const hoverChangeRef = useRef(onHoverChange);
  const readyRef = useRef(onReady);
  const playingChangeRef = useRef(onPlayingChange);
  selectionChangeRef.current = onSelectionChange;
  cursorChangeRef.current = onCursorChange;
  hoverChangeRef.current = onHoverChange;
  readyRef.current = onReady;
  playingChangeRef.current = onPlayingChange;

  const [state, setState] = useState<"loading" | "ready" | "error">("loading");
  const [error, setError] = useState<string | null>(null);

  const isAbortLike = (err: unknown) => {
    const msg = (err instanceof Error ? err.message : String(err ?? "")).toLowerCase();
    return msg.includes("abort") || msg.includes("cancel");
  };

  // Create the instance once.
  useEffect(() => {
    if (!containerRef.current) return;

    const isDark = document.documentElement.classList.contains("dark");
    const regions = RegionsPlugin.create();
    const ws = WaveSurfer.create({
      container: containerRef.current,
      height: 200,
      normalize: true,
      waveColor: isDark ? "#454545" : "#cfcfcf",
      progressColor: isDark ? "#9a9a9a" : "#6b6b6b",
      cursorColor: isDark ? "#f5f5f5" : "#171717",
      cursorWidth: 1,
      barWidth: 2,
      barGap: 1,
      dragToSeek: false,
      interact: true,
      autoScroll: false,
      autoCenter: false,
      plugins: [regions],
    });

    waveSurferRef.current = ws;
    regionsRef.current = regions;
    readyRef.current?.(ws);

    const timeAtClientX = (clientX: number): number | null => {
      const wrapper = ws.getWrapper();
      const scroll = wrapper.parentElement;
      const duration = ws.getDuration();
      if (!scroll || duration <= 0 || wrapper.scrollWidth <= 0) return null;
      const rect = scroll.getBoundingClientRect();
      const localX = Math.max(0, Math.min(clientX - rect.left, rect.width));
      const absX = Math.max(0, Math.min(scroll.scrollLeft + localX, wrapper.scrollWidth));
      return Number(((absX / wrapper.scrollWidth) * duration).toFixed(3));
    };

    ws.on("timeupdate", (t) => {
      if (ws.isPlaying()) cursorChangeRef.current(Number(t.toFixed(2)));
    });
    ws.on("interaction", () => {
      if (!draggedRef.current) cursorChangeRef.current(Number(ws.getCurrentTime().toFixed(2)));
    });
    ws.on("click", () => {
      if (draggedRef.current) return;
      const t = Number(ws.getCurrentTime().toFixed(2));
      cursorChangeRef.current(t);
      selectionChangeRef.current(t, t);
    });
    ws.on("play", () => playingChangeRef.current?.(true));
    ws.on("pause", () => playingChangeRef.current?.(false));
    ws.on("finish", () => playingChangeRef.current?.(false));
    ws.on("ready", () => {
      setState("ready");
      setError(null);
    });
    ws.on("error", (err) => {
      if (isAbortLike(err)) return;
      setState("error");
      setError(err instanceof Error ? err.message : "Audio failed to load.");
      playingChangeRef.current?.(false);
    });

    regions.on("region-created", (region) => {
      for (const other of regions.getRegions()) {
        if (other.id !== region.id) other.remove();
      }
      selectionChangeRef.current(Number(region.start.toFixed(3)), Number(region.end.toFixed(3)));
    });
    regions.on("region-updated", (region) => {
      selectionChangeRef.current(Number(region.start.toFixed(3)), Number(region.end.toFixed(3)));
    });

    const el = containerRef.current;
    const onPointerDown = (e: PointerEvent) => {
      pointerDownRef.current = true;
      pointerStartXRef.current = e.clientX;
      pointerStartTimeRef.current = timeAtClientX(e.clientX);
      draggedRef.current = false;
    };
    const onPointerMove = (e: PointerEvent) => {
      const t = timeAtClientX(e.clientX);
      if (t !== null) hoverChangeRef.current?.(t);
      if (pointerDownRef.current && pointerStartXRef.current !== null) {
        if (Math.abs(e.clientX - pointerStartXRef.current) > 4) draggedRef.current = true;
      }
    };
    const onPointerUp = (e: PointerEvent) => {
      const didDrag = draggedRef.current;
      const start = pointerStartTimeRef.current;
      const end = timeAtClientX(e.clientX);
      pointerDownRef.current = false;
      pointerStartXRef.current = null;
      pointerStartTimeRef.current = null;
      if (didDrag && start !== null && end !== null) {
        selectionChangeRef.current(Math.min(start, end), Math.max(start, end));
        setTimeout(() => {
          draggedRef.current = false;
        }, 120);
      } else {
        draggedRef.current = false;
      }
    };
    const onWheel = (e: WheelEvent) => {
      if (!e.ctrlKey) return;
      e.preventDefault();
      const base = zoomRef.current || 80;
      zoomRef.current = Math.max(20, Math.min(600, base + (e.deltaY < 0 ? 30 : -30)));
      ws.zoom(zoomRef.current);
    };
    const onLeave = () => hoverChangeRef.current?.(null);

    el.addEventListener("wheel", onWheel, { passive: false });
    el.addEventListener("pointerdown", onPointerDown);
    el.addEventListener("pointermove", onPointerMove);
    el.addEventListener("pointerup", onPointerUp);
    el.addEventListener("pointerleave", onLeave);

    return () => {
      el.removeEventListener("wheel", onWheel);
      el.removeEventListener("pointerdown", onPointerDown);
      el.removeEventListener("pointermove", onPointerMove);
      el.removeEventListener("pointerup", onPointerUp);
      el.removeEventListener("pointerleave", onLeave);
      readyRef.current?.(null);
      ws.destroy();
      waveSurferRef.current = null;
      regionsRef.current = null;
    };
  }, []);

  // Load audio when the URL changes.
  useEffect(() => {
    const ws = waveSurferRef.current;
    if (!ws || !audioUrl) return;
    setState("loading");
    setError(null);
    zoomRef.current = 0;
    void ws.load(audioUrl).catch((err) => {
      if (!isAbortLike(err)) {
        setState("error");
        setError(err instanceof Error ? err.message : "Audio failed to load.");
      }
    });
  }, [audioUrl]);

  // Reflect external selection into a region.
  useEffect(() => {
    const regions = regionsRef.current;
    if (!regions) return;
    const start = Math.min(selectionStart, selectionEnd);
    const end = Math.max(selectionStart, selectionEnd);
    const current = regions.getRegions()[0];
    if (end <= start + 0.01) {
      current?.remove();
      return;
    }
    if (!current) {
      regions.addRegion({
        start,
        end,
        color: "hsl(var(--primary) / 0.12)",
        drag: true,
        resize: true,
      });
    } else if (Math.abs(current.start - start) > 0.02 || Math.abs(current.end - end) > 0.02) {
      current.setOptions({ start, end });
    }
  }, [selectionStart, selectionEnd]);

  return (
    <div className="relative">
      {/* lineHeight/fontSize 0: line-height is inherited into WaveSurfer's
          shadow DOM, where whitespace text nodes in its template would
          otherwise create phantom line boxes that inflate the scroll/wrapper
          height and push the main canvas below the (absolutely positioned)
          progress canvas — producing a vertical step at the playback cursor. */}
      <div
        ref={containerRef}
        className="w-full"
        style={{ lineHeight: 0, fontSize: 0 }}
        aria-label="Waveform editor"
      />
      {state !== "ready" ? (
        <div
          className="absolute inset-0 flex flex-col items-center justify-center gap-1 bg-secondary/40 text-center"
          role={state === "error" ? "alert" : "status"}
        >
          <span className="text-sm">
            {state === "loading" ? "Loading audio…" : "Audio unavailable"}
          </span>
          {state === "error" ? (
            <span className="max-w-xs text-xs text-muted-foreground">{error}</span>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
