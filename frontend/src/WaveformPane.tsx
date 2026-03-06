import { useEffect, useRef } from "react";
import WaveSurfer from "wavesurfer.js";
import RegionsPlugin from "wavesurfer.js/dist/plugins/regions.esm.js";

type WaveformPaneProps = {
  audioUrl: string;
  durationSeconds: number;
  peaks: number[] | null;
  selectionStart: number;
  selectionEnd: number;
  onSelectionChange: (start: number, end: number) => void;
  onCursorChange: (time: number) => void;
  onReady?: (instance: WaveSurfer | null) => void;
  onPlayingChange?: (isPlaying: boolean) => void;
};

export default function WaveformPane({
  audioUrl,
  durationSeconds,
  peaks,
  selectionStart,
  selectionEnd,
  onSelectionChange,
  onCursorChange,
  onReady,
  onPlayingChange,
}: WaveformPaneProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const waveSurferRef = useRef<WaveSurfer | null>(null);
  const regionsRef = useRef<any>(null);
  const zoomRef = useRef(90);
  const isPointerDownRef = useRef(false);
  const pointerStartXRef = useRef<number | null>(null);
  const draggedThisGestureRef = useRef(false);
  const selectionChangeRef = useRef(onSelectionChange);
  const cursorChangeRef = useRef(onCursorChange);
  const readyRef = useRef(onReady);
  const playingChangeRef = useRef(onPlayingChange);

  useEffect(() => {
    selectionChangeRef.current = onSelectionChange;
    cursorChangeRef.current = onCursorChange;
    readyRef.current = onReady;
    playingChangeRef.current = onPlayingChange;
  }, [onSelectionChange, onCursorChange, onReady, onPlayingChange]);

  useEffect(() => {
    if (!containerRef.current) {
      return;
    }

    const regions = RegionsPlugin.create();
    const waveSurfer = WaveSurfer.create({
      container: containerRef.current,
      height: 220,
      normalize: true,
      waveColor: "#78d2cd",
      progressColor: "#3c8d9f",
      cursorColor: "#f3a545",
      cursorWidth: 2,
      minPxPerSec: zoomRef.current,
      dragToSeek: false,
      interact: true,
      autoScroll: false,
      autoCenter: false,
      hideScrollbar: false,
      plugins: [regions],
    });

    waveSurferRef.current = waveSurfer;
    regionsRef.current = regions;
    readyRef.current?.(waveSurfer);

    regions.enableDragSelection({
      color: "rgba(247, 203, 104, 0.2)",
    });

    const handleTimeUpdate = (time: number) => {
      if (!waveSurfer.isPlaying()) {
        return;
      }
      cursorChangeRef.current(Number(time.toFixed(2)));

      // Audacity-like page scrolling: jump viewport when playhead exits visible window.
      const scrollContainer = waveSurfer.getWrapper().parentElement;
      const duration = waveSurfer.getDuration();
      if (!scrollContainer || duration <= 0) {
        return;
      }

      const totalWidth = waveSurfer.getWrapper().scrollWidth;
      const viewportWidth = scrollContainer.clientWidth;
      if (totalWidth <= viewportWidth) {
        return;
      }

      const currentPx = (time / duration) * totalWidth;
      const viewStart = scrollContainer.scrollLeft;
      const viewEnd = viewStart + viewportWidth;

      if (currentPx > viewEnd - 2) {
        const nextPageStart =
          Math.floor(currentPx / viewportWidth) * viewportWidth;
        scrollContainer.scrollLeft = Math.min(
          nextPageStart,
          Math.max(totalWidth - viewportWidth, 0),
        );
      } else if (currentPx < viewStart) {
        const prevPageStart =
          Math.floor(currentPx / viewportWidth) * viewportWidth;
        scrollContainer.scrollLeft = Math.max(prevPageStart, 0);
      }
    };
    const handleInteraction = () => {
      if (draggedThisGestureRef.current) {
        return;
      }
      cursorChangeRef.current(Number(waveSurfer.getCurrentTime().toFixed(2)));
    };
    const handleSeeking = (time: number) => {
      if (draggedThisGestureRef.current) {
        return;
      }
      cursorChangeRef.current(Number(time.toFixed(2)));
    };
    const handleClick = () => {
      if (draggedThisGestureRef.current) {
        return;
      }
      const time = Number(waveSurfer.getCurrentTime().toFixed(2));
      cursorChangeRef.current(time);
      selectionChangeRef.current(time, time);
    };

    waveSurfer.on("timeupdate", handleTimeUpdate);
    waveSurfer.on("interaction", handleInteraction);
    waveSurfer.on("seeking", handleSeeking);
    waveSurfer.on("click", handleClick);
    waveSurfer.on("play", () => playingChangeRef.current?.(true));
    waveSurfer.on("pause", () => playingChangeRef.current?.(false));
    waveSurfer.on("finish", () => playingChangeRef.current?.(false));

    regions.on("region-created", (region: any) => {
      for (const candidate of regions.getRegions()) {
        if (candidate.id !== region.id) {
          candidate.remove();
        }
      }
      selectionChangeRef.current(
        Number(region.start.toFixed(2)),
        Number(region.end.toFixed(2)),
      );
    });

    regions.on("region-updated", (region: any) => {
      selectionChangeRef.current(
        Number(region.start.toFixed(2)),
        Number(region.end.toFixed(2)),
      );
    });

    const handlePointerDown = (event: PointerEvent) => {
      isPointerDownRef.current = true;
      pointerStartXRef.current = event.clientX;
      draggedThisGestureRef.current = false;
    };

    const handlePointerMove = (event: PointerEvent) => {
      if (!isPointerDownRef.current || pointerStartXRef.current === null) {
        return;
      }
      if (Math.abs(event.clientX - pointerStartXRef.current) > 4) {
        draggedThisGestureRef.current = true;
      }
    };

    const handlePointerUp = () => {
      isPointerDownRef.current = false;
      pointerStartXRef.current = null;
      // Keep this true briefly so the click event right after drag is ignored.
      if (!draggedThisGestureRef.current) {
        return;
      }
      setTimeout(() => {
        draggedThisGestureRef.current = false;
      }, 120);
    };

    const handleWheel = (event: WheelEvent) => {
      const scrollContainer = waveSurfer.getWrapper().parentElement;

      if (event.ctrlKey) {
        event.preventDefault();
        const delta = event.deltaY < 0 ? 36 : -36;
        zoomRef.current = Math.max(30, Math.min(1000, zoomRef.current + delta));
        waveSurfer.zoom(zoomRef.current);
        return;
      }

      if (event.shiftKey && scrollContainer) {
        event.preventDefault();
        scrollContainer.scrollLeft += event.deltaY;
      }
    };

    containerRef.current.addEventListener("wheel", handleWheel, { passive: false });
    containerRef.current.addEventListener("pointerdown", handlePointerDown);
    containerRef.current.addEventListener("pointermove", handlePointerMove);
    containerRef.current.addEventListener("pointerup", handlePointerUp);
    containerRef.current.addEventListener("pointercancel", handlePointerUp);

    return () => {
      containerRef.current?.removeEventListener("wheel", handleWheel);
      containerRef.current?.removeEventListener("pointerdown", handlePointerDown);
      containerRef.current?.removeEventListener("pointermove", handlePointerMove);
      containerRef.current?.removeEventListener("pointerup", handlePointerUp);
      containerRef.current?.removeEventListener("pointercancel", handlePointerUp);
      readyRef.current?.(null);
      waveSurfer.destroy();
      waveSurferRef.current = null;
      regionsRef.current = null;
    };
  }, []);

  useEffect(() => {
    const waveSurfer = waveSurferRef.current;
    if (!waveSurfer) {
      return;
    }

    const resetOnReady = () => {
      waveSurfer.seekTo(0);
      cursorChangeRef.current(0);
    };
    waveSurfer.once("ready", resetOnReady);

    if (peaks && peaks.length > 0) {
      void waveSurfer.load(audioUrl, [peaks], durationSeconds);
      return;
    }
    void waveSurfer.load(audioUrl);
  }, [audioUrl, durationSeconds, peaks]);

  useEffect(() => {
    const regions = regionsRef.current;
    if (!regions) {
      return;
    }

    const start = Math.min(selectionStart, selectionEnd);
    const end = Math.max(selectionStart, selectionEnd);
    const currentRegion = regions.getRegions()[0];

    if (end <= start + 0.01) {
      if (currentRegion) {
        currentRegion.remove();
      }
      return;
    }

    if (!currentRegion) {
      regions.addRegion({
        start,
        end,
        color: "rgba(247, 203, 104, 0.2)",
        drag: true,
        resize: true,
      });
      return;
    }

    if (
      Math.abs(currentRegion.start - start) > 0.02 ||
      Math.abs(currentRegion.end - end) > 0.02
    ) {
      currentRegion.setOptions({ start, end });
    }
  }, [selectionStart, selectionEnd]);

  return <div ref={containerRef} className="wavesurfer-host" aria-label="Waveform editor" />;
}
