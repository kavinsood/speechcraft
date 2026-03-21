export type ReferenceTrimSuggestion = {
  startOffsetSeconds: number;
  endOffsetSeconds: number;
  previewDurationSeconds: number;
  heuristic: string;
};

export type ReferenceTrimValidationResult = {
  trim: ReferenceTrimSuggestion | null;
  error: string | null;
};

const LEADING_MARGIN_SECONDS = 0.08;
const TRAILING_MARGIN_SECONDS = 0.16;
const MIN_DURATION_SECONDS = 0.12;
const FRAME_SECONDS = 0.02;
const MIN_ACTIVE_FRAMES = 2;
const MAX_GAP_FRAMES = 3;

export function clampTrimOffsets(
  startOffsetSeconds: number,
  endOffsetSeconds: number,
  previewDurationSeconds: number,
): ReferenceTrimSuggestion {
  const duration = Number.isFinite(previewDurationSeconds)
    ? Math.max(previewDurationSeconds, MIN_DURATION_SECONDS)
    : MIN_DURATION_SECONDS;
  const safeStart = Number.isFinite(startOffsetSeconds) ? startOffsetSeconds : 0;
  const safeEnd = Number.isFinite(endOffsetSeconds) ? endOffsetSeconds : duration;
  const clampedStart = Math.max(0, Math.min(safeStart, duration));
  const clampedEnd = Math.max(clampedStart + MIN_DURATION_SECONDS, Math.min(safeEnd, duration));
  return {
    startOffsetSeconds: clampedStart,
    endOffsetSeconds: clampedEnd,
    previewDurationSeconds: duration,
    heuristic: "manual",
  };
}

export function validateManualTrimOffsets(
  startOffsetSeconds: number,
  endOffsetSeconds: number,
  previewDurationSeconds: number,
): ReferenceTrimValidationResult {
  if (
    !Number.isFinite(startOffsetSeconds)
    || !Number.isFinite(endOffsetSeconds)
    || !Number.isFinite(previewDurationSeconds)
  ) {
    return { trim: null, error: "Enter valid numeric trim bounds." };
  }

  const duration = Math.max(previewDurationSeconds, MIN_DURATION_SECONDS);
  if (startOffsetSeconds < 0 || startOffsetSeconds > duration) {
    return { trim: null, error: "Trim start must stay inside the candidate." };
  }
  if (endOffsetSeconds < 0 || endOffsetSeconds > duration) {
    return { trim: null, error: "Trim end must stay inside the candidate." };
  }
  if (endOffsetSeconds <= startOffsetSeconds) {
    return { trim: null, error: "Trim end must be later than trim start." };
  }
  if (endOffsetSeconds - startOffsetSeconds < MIN_DURATION_SECONDS) {
    return { trim: null, error: `Trim must keep at least ${MIN_DURATION_SECONDS.toFixed(2)}s of audio.` };
  }

  return {
    trim: {
      startOffsetSeconds,
      endOffsetSeconds,
      previewDurationSeconds: duration,
      heuristic: "manual",
    },
    error: null,
  };
}

export async function computeReferenceTrimSuggestion(audioUrl: string): Promise<ReferenceTrimSuggestion> {
  const response = await fetch(audioUrl);
  if (!response.ok) {
    throw new Error(`Failed to fetch preview audio: ${response.status}`);
  }
  const audioBytes = await response.arrayBuffer();
  const audioContext = createAudioContext();
  try {
    const decoded = await audioContext.decodeAudioData(audioBytes.slice(0));
    return suggestTrimBoundsFromAudioBuffer(decoded);
  } finally {
    await audioContext.close();
  }
}

function createAudioContext(): AudioContext {
  const AudioContextCtor = window.AudioContext ?? (window as Window & { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
  if (!AudioContextCtor) {
    throw new Error("WebAudio is unavailable in this browser");
  }
  return new AudioContextCtor();
}

function suggestTrimBoundsFromAudioBuffer(buffer: AudioBuffer): ReferenceTrimSuggestion {
  const previewDurationSeconds = buffer.duration;
  if (!Number.isFinite(previewDurationSeconds) || previewDurationSeconds <= MIN_DURATION_SECONDS) {
    return {
      startOffsetSeconds: 0,
      endOffsetSeconds: Math.max(previewDurationSeconds, MIN_DURATION_SECONDS),
      previewDurationSeconds: Math.max(previewDurationSeconds, MIN_DURATION_SECONDS),
      heuristic: "full-span",
    };
  }

  const mono = mixToMono(buffer);
  const frameSize = Math.max(1, Math.round(buffer.sampleRate * FRAME_SECONDS));
  const rmsFrames = computeRmsFrames(mono, frameSize);
  const threshold = deriveRmsThreshold(rmsFrames);
  const activeFrames = smoothActivityMask(
    rmsFrames.map((value) => value >= threshold),
  );
  const firstActiveFrame = activeFrames.findIndex(Boolean);
  const lastActiveFrame = activeFrames.lastIndexOf(true);
  if (firstActiveFrame === -1 || lastActiveFrame === -1) {
    return {
      startOffsetSeconds: 0,
      endOffsetSeconds: previewDurationSeconds,
      previewDurationSeconds,
      heuristic: "full-span",
    };
  }

  const startOffsetSeconds = Math.max(0, firstActiveFrame * FRAME_SECONDS - LEADING_MARGIN_SECONDS);
  const endOffsetSeconds = Math.min(
    previewDurationSeconds,
    (lastActiveFrame + 1) * FRAME_SECONDS + TRAILING_MARGIN_SECONDS,
  );
  return {
    ...clampTrimOffsets(startOffsetSeconds, endOffsetSeconds, previewDurationSeconds),
    heuristic: "rms-boundary",
  };
}

function mixToMono(buffer: AudioBuffer): Float32Array {
  if (buffer.numberOfChannels === 1) {
    return buffer.getChannelData(0).slice();
  }
  const mixed = new Float32Array(buffer.length);
  for (let channelIndex = 0; channelIndex < buffer.numberOfChannels; channelIndex += 1) {
    const channel = buffer.getChannelData(channelIndex);
    for (let frameIndex = 0; frameIndex < channel.length; frameIndex += 1) {
      mixed[frameIndex] += channel[frameIndex];
    }
  }
  for (let frameIndex = 0; frameIndex < mixed.length; frameIndex += 1) {
    mixed[frameIndex] /= buffer.numberOfChannels;
  }
  return mixed;
}

function computeRmsFrames(samples: Float32Array, frameSize: number): number[] {
  const frames: number[] = [];
  for (let startIndex = 0; startIndex < samples.length; startIndex += frameSize) {
    const endIndex = Math.min(samples.length, startIndex + frameSize);
    let sumSquares = 0;
    for (let sampleIndex = startIndex; sampleIndex < endIndex; sampleIndex += 1) {
      const sample = samples[sampleIndex];
      sumSquares += sample * sample;
    }
    frames.push(Math.sqrt(sumSquares / Math.max(1, endIndex - startIndex)));
  }
  return frames;
}

function deriveRmsThreshold(frames: number[]): number {
  if (frames.length === 0) {
    return 0;
  }
  const sorted = [...frames].sort((left, right) => left - right);
  const noiseFloor = sorted[Math.floor(sorted.length * 0.2)] ?? 0;
  const peak = sorted[sorted.length - 1] ?? 0;
  return Math.max(noiseFloor * 2.5, peak * 0.12, 0.004);
}

function smoothActivityMask(activity: boolean[]): boolean[] {
  const next = [...activity];
  let runStart = -1;
  for (let index = 0; index < next.length; index += 1) {
    if (next[index]) {
      if (runStart === -1) {
        runStart = index;
      }
      continue;
    }
    if (runStart !== -1 && index - runStart < MIN_ACTIVE_FRAMES) {
      for (let fillIndex = runStart; fillIndex < index; fillIndex += 1) {
        next[fillIndex] = false;
      }
    }
    runStart = -1;
  }
  if (runStart !== -1 && next.length - runStart < MIN_ACTIVE_FRAMES) {
    for (let fillIndex = runStart; fillIndex < next.length; fillIndex += 1) {
      next[fillIndex] = false;
    }
  }

  let gapStart = -1;
  for (let index = 0; index < next.length; index += 1) {
    if (!next[index]) {
      if (gapStart === -1) {
        gapStart = index;
      }
      continue;
    }
    if (gapStart !== -1 && index - gapStart <= MAX_GAP_FRAMES && gapStart > 0) {
      for (let fillIndex = gapStart; fillIndex < index; fillIndex += 1) {
        next[fillIndex] = true;
      }
    }
    gapStart = -1;
  }
  return next;
}
