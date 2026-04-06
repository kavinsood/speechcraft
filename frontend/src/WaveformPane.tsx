import { useEffect, useRef, useState } from "react";
import WaveSurfer from "wavesurfer.js";
import RegionsPlugin from "wavesurfer.js/dist/plugins/regions.esm.js";

type WaveformPaneProps = {
  audioUrl: string;
  durationSeconds: number;
  peaks: number[] | null;
  desiredCursorSeconds?: number;
  selectionStart: number;
  selectionEnd: number;
  onSelectionChange: (start: number, end: number) => void;
  onCursorChange: (time: number) => void;
  onHoverTimeChange?: (time: number | null) => void;
  onReady?: (instance: WaveSurfer | null) => void;
  onPlayingChange?: (isPlaying: boolean) => void;
};

export default function WaveformPane({
  audioUrl,
  durationSeconds,
  peaks,
  desiredCursorSeconds = 0,
  selectionStart,
  selectionEnd,
  onSelectionChange,
  onCursorChange,
  onHoverTimeChange,
  onReady,
  onPlayingChange,
}: WaveformPaneProps) {
  const precision = 10000;
  const roundTime = (value: number): number => Math.round(value * precision) / precision;
  const containerRef = useRef<HTMLDivElement | null>(null);
  const waveSurferRef = useRef<WaveSurfer | null>(null);
  const regionsRef = useRef<any>(null);
  const zoomRef = useRef(90);
  const isPointerDownRef = useRef(false);
  const pointerStartXRef = useRef<number | null>(null);
  const draggedThisGestureRef = useRef(false);
  const selectionChangeRef = useRef(onSelectionChange);
  const cursorChangeRef = useRef(onCursorChange);
  const hoverTimeChangeRef = useRef(onHoverTimeChange);
  const readyRef = useRef(onReady);
  const playingChangeRef = useRef(onPlayingChange);
  const lastAudioUrlRef = useRef<string | null>(null);
  const desiredCursorRef = useRef(desiredCursorSeconds);
  const [audioState, setAudioState] = useState<"loading" | "ready" | "error">("loading");
  const [audioError, setAudioError] = useState<string | null>(null);

  function isAbortLikeError(error: unknown): boolean {
    const message =
      error instanceof Error
        ? error.message
        : typeof error === "string"
          ? error
          : "";
    const normalized = message.toLowerCase();
    return (
      normalized.includes("abort") ||
      normalized.includes("aborted") ||
      normalized.includes("user aborted") ||
      normalized.includes("cancel")
    );
  }

  useEffect(() => {
    selectionChangeRef.current = onSelectionChange;
    cursorChangeRef.current = onCursorChange;
    hoverTimeChangeRef.current = onHoverTimeChange;
    readyRef.current = onReady;
    playingChangeRef.current = onPlayingChange;
  }, [onSelectionChange, onCursorChange, onHoverTimeChange, onReady, onPlayingChange]);

  useEffect(() => {
    desiredCursorRef.current = desiredCursorSeconds;
  }, [desiredCursorSeconds]);

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
      cursorChangeRef.current(roundTime(waveSurfer.getCurrentTime()));
    };
    const handleSeeking = (time: number) => {
      if (draggedThisGestureRef.current) {
        return;
      }
      cursorChangeRef.current(roundTime(time));
    };
    const handleClick = () => {
      if (draggedThisGestureRef.current) {
        return;
      }
      const time = roundTime(waveSurfer.getCurrentTime());
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
    waveSurfer.on("ready", () => {
      setAudioState("ready");
      setAudioError(null);
    });
    waveSurfer.on("error", (error) => {
      if (isAbortLikeError(error)) {
        return;
      }
      const message =
        error instanceof Error
          ? error.message
          : typeof error === "string" && error
            ? error
            : "Audio failed to load for this clip.";
      setAudioState("error");
      setAudioError(message);
      playingChangeRef.current?.(false);
    });

    regions.on("region-created", (region: any) => {
      for (const candidate of regions.getRegions()) {
        if (candidate.id !== region.id) {
          candidate.remove();
        }
      }
      selectionChangeRef.current(
        roundTime(region.start),
        roundTime(region.end),
      );
    });

    regions.on("region-updated", (region: any) => {
      selectionChangeRef.current(
        roundTime(region.start),
        roundTime(region.end),
      );
    });

    const handlePointerDown = (event: PointerEvent) => {
      isPointerDownRef.current = true;
      pointerStartXRef.current = event.clientX;
      draggedThisGestureRef.current = false;
    };

    const handlePointerMove = (event: PointerEvent) => {
      const wrapper = waveSurfer.getWrapper();
      const scrollContainer = wrapper.parentElement;
      const duration = waveSurfer.getDuration();
      if (scrollContainer && duration > 0 && wrapper.scrollWidth > 0) {
        const viewport = scrollContainer.getBoundingClientRect();
        const localX = Math.max(0, Math.min(event.clientX - viewport.left, viewport.width));
        const absoluteX = Math.max(
          0,
          Math.min(scrollContainer.scrollLeft + localX, wrapper.scrollWidth),
        );
        const hoverTime = roundTime((absoluteX / wrapper.scrollWidth) * duration);
        hoverTimeChangeRef.current?.(hoverTime);
      }

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
    const handlePointerLeave = () => {
      hoverTimeChangeRef.current?.(null);
    };

    containerRef.current.addEventListener("wheel", handleWheel, { passive: false });
    containerRef.current.addEventListener("pointerdown", handlePointerDown);
    containerRef.current.addEventListener("pointermove", handlePointerMove);
    containerRef.current.addEventListener("pointerup", handlePointerUp);
    containerRef.current.addEventListener("pointercancel", handlePointerUp);
    containerRef.current.addEventListener("pointerleave", handlePointerLeave);

    return () => {
      containerRef.current?.removeEventListener("wheel", handleWheel);
      containerRef.current?.removeEventListener("pointerdown", handlePointerDown);
      containerRef.current?.removeEventListener("pointermove", handlePointerMove);
      containerRef.current?.removeEventListener("pointerup", handlePointerUp);
      containerRef.current?.removeEventListener("pointercancel", handlePointerUp);
      containerRef.current?.removeEventListener("pointerleave", handlePointerLeave);
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

    const isClipChange = lastAudioUrlRef.current !== audioUrl;
    lastAudioUrlRef.current = audioUrl;
    const targetTime = isClipChange
      ? 0
      : Math.max(0, Math.min(desiredCursorRef.current, durationSeconds));
    const seekOnReady = () => {
      const duration = waveSurfer.getDuration();
      if (duration > 0) {
        waveSurfer.seekTo(Math.max(0, Math.min(targetTime / duration, 1)));
      }
      cursorChangeRef.current(roundTime(targetTime));
    };
    waveSurfer.once("ready", seekOnReady);
    setAudioState("loading");
    setAudioError(null);

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

  return (
    <div className={`waveform-shell waveform-shell-${audioState}`}>
      <div ref={containerRef} className="wavesurfer-host" aria-label="Waveform editor" />
      {audioState !== "ready" ? (
        <div
          className={`waveform-overlay waveform-overlay-${audioState}`}
          role={audioState === "error" ? "alert" : "status"}
        >
          <strong>{audioState === "loading" ? "Loading audio..." : "Audio unavailable"}</strong>
          <span>
            {audioState === "loading"
              ? "Fetching clip audio and preparing the waveform."
              : audioError ?? "The backend could not decode this clip."}
          </span>
        </div>
      ) : null}
    </div>
  );
}
