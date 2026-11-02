/** Lazy, browser-only per-scenario export. Uses the player's exact paint callback. */
import type {Drawing} from './animation-player.ts';

type Paint = (ctx: CanvasRenderingContext2D, drawing: Drawing, time: number, theme?: string, dark?: boolean) => void;
export interface ExportOptions {
  drawing: Drawing; paint: Paint; format: 'gif' | 'video'; speed: number;
  theme?: string; dark?: boolean; signal: AbortSignal; onProgress: (fraction: number) => void;
  gifWorkerURL?: string;
}
export const EXPORT_LIMITS = {gif: {seconds: 600, frames: 1200, bytes: 64 * 1024 * 1024},
  video: {seconds: 600, bytes: 128 * 1024 * 1024}} as const;
const WIDTH = 1920, HEIGHT = 1080, FPS = 24;
const canceled = () => new DOMException('Export canceled.', 'AbortError');
const check = (signal: AbortSignal) => {if (signal.aborted) throw canceled();};

/** Availability is browser/OS-dependent. A WebM fallback is never mislabeled MP4. */
export function videoSupport(): {mimeType: string; extension: 'mp4' | 'webm'; label: string} | null {
  if (typeof MediaRecorder === 'undefined' || typeof HTMLCanvasElement === 'undefined'
    || !HTMLCanvasElement.prototype.captureStream) return null;
  for (const mimeType of ['video/mp4;codecs=avc1.420028', 'video/mp4', 'video/webm;codecs=vp9', 'video/webm;codecs=vp8', 'video/webm']) {
    if (MediaRecorder.isTypeSupported(mimeType)) {
      const extension = mimeType.startsWith('video/mp4') ? 'mp4' : 'webm';
      return {mimeType, extension, label: extension.toUpperCase()};
    }
  }
  return null;
}

export function exportDuration(drawing: Drawing, speed: number): number {
  if (!Number.isFinite(speed) || speed < .1 || speed > 16) throw new Error('Choose a speed between 0.1× and 16×.');
  const duration = drawing.frames.reduce((sum, frame) => sum + frame.duration, 0) / speed;
  if (!Number.isFinite(duration) || duration <= 0) throw new Error('This scenario has no duration to export.');
  return duration;
}

/** Static holds need only one GIF frame. Motion is sampled without skipping state boundaries. */
export function gifSchedule(drawing: Drawing, speed: number): {time: number; delay: number}[] {
  if (exportDuration(drawing, speed) > EXPORT_LIMITS.gif.seconds) throw new Error('GIF is limited to 10 minutes. Increase playback speed.');
  const segments: {time: number; duration: number; items: number[]}[] = [];
  let time = 0;
  for (const frame of drawing.frames) {
    const previous = segments.at(-1);
    if (!drawing.motions?.length && previous && frame.items.length === previous.items.length && frame.items.every((id, i) => id === previous.items[i])) previous.duration += frame.duration;
    else segments.push({time, duration: frame.duration, items: frame.items});
    time += frame.duration;
  }
  const frames: {time: number; delay: number}[] = [];
  for (const segment of segments) {
    const count = drawing.motions?.length ? Math.ceil(segment.duration / speed * 10) : 1;
    if (frames.length + count > EXPORT_LIMITS.gif.frames) throw new Error('GIF is limited to 1,200 encoded frames. Increase playback speed or choose video.');
    for (let i = 0; i < count; i++) {
      const start = segment.time + segment.duration * i / count, end = segment.time + segment.duration * (i + 1) / count;
      const delay = (Math.round(end / speed * 100) - Math.round(start / speed * 100)) * 10;
      if (delay < 20) throw new Error('A state is shorter than GIF’s portable 0.02-second minimum. Lower playback speed to retain it.');
      frames.push({time: start, delay});
    }
  }
  return frames;
}

export function videoBoundaries(drawing: Drawing, speed: number): number[] {
  if (exportDuration(drawing, speed) > EXPORT_LIMITS.video.seconds) throw new Error('Video is limited to 10 minutes. Increase playback speed.');
  let time = 0;
  return drawing.frames.map(frame => {
    if (frame.duration / speed < 1 / FPS) throw new Error('A state is shorter than one 24 fps video frame. Lower playback speed or choose GIF.');
    const start = time; time += frame.duration / speed; return start;
  });
}

/** Abort instead of silently losing a whole evidence state after browser scheduling delays. */
export function videoState(starts: number[], elapsed: number, previous: number): number {
  let index = previous;
  while (index + 1 < starts.length && starts[index + 1] <= elapsed) index++;
  if (index > previous + 1) throw new Error('The browser skipped a visual state while recording. Lower playback speed and retry, or choose GIF.');
  return index;
}

async function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  check(signal);
  return new Promise((resolve, reject) => {
    const abort = () => reject(canceled());
    signal.addEventListener('abort', abort, {once: true});
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', abort));
  });
}

async function gif(context: CanvasRenderingContext2D, render: (time: number) => void, options: ExportOptions): Promise<Blob> {
  const frames = gifSchedule(options.drawing, options.speed);
  if (!options.gifWorkerURL || typeof Worker === 'undefined') throw new Error('GIF export is unavailable in this browser.');
  const url = new URL(options.gifWorkerURL, location.href);
  if (url.origin !== location.origin) throw new Error('The GIF worker must be hosted on this site.');
  const worker = new Worker(url, {type: 'module'});
  let rejectPending: ((reason: unknown) => void) | undefined, timeout: ReturnType<typeof setTimeout> | undefined;
  const abort = () => {worker.terminate(); rejectPending?.(canceled());};
  options.signal.addEventListener('abort', abort, {once: true});
  const send = (message: object, transfer: Transferable[] = []): Promise<ArrayBuffer | undefined> => new Promise((resolve, reject) => {
    rejectPending = reject;
    timeout = setTimeout(() => reject(new Error('GIF encoding timed out. Try a shorter scenario or video.')), 60_000);
    worker.onmessage = event => {clearTimeout(timeout); rejectPending = undefined; event.data.error ? reject(new Error(event.data.error)) : resolve(event.data.buffer);};
    worker.onerror = event => {clearTimeout(timeout); rejectPending = undefined; reject(new Error(event.message || 'GIF encoding failed.'));};
    worker.postMessage(message, transfer);
  });
  try {
    check(options.signal);
    await send({type: 'init', width: WIDTH, height: HEIGHT, maxBytes: EXPORT_LIMITS.gif.bytes});
    for (const [index, frame] of frames.entries()) {
      check(options.signal); render(frame.time);
      const pixels = context.getImageData(0, 0, WIDTH, HEIGHT).data;
      // Exactly one RGBA frame in flight; palette work never blocks the UI thread.
      await send({type: 'frame', buffer: pixels.buffer, delay: frame.delay}, [pixels.buffer]);
      options.onProgress(.98 * (index + 1) / frames.length);
    }
    const buffer = await send({type: 'finish'});
    if (!buffer) throw new Error('GIF encoder produced no output.');
    return new Blob([buffer], {type: 'image/gif'});
  } finally {clearTimeout(timeout); options.signal.removeEventListener('abort', abort); worker.terminate();}
}

async function video(canvas: HTMLCanvasElement, render: (time: number) => void, options: ExportOptions): Promise<Blob> {
  const support = videoSupport(), duration = exportDuration(options.drawing, options.speed);
  if (!support) throw new Error('Video recording is unavailable. Try GIF or another browser.');
  const starts = videoBoundaries(options.drawing, options.speed);
  if (document.hidden) throw new Error('Keep this tab visible during the real-time video export.');
  render(0);
  const stream = canvas.captureStream(FPS);
  try {
    return await new Promise<Blob>((resolve, reject) => {
      const recorder = new MediaRecorder(stream, {mimeType: support.mimeType, videoBitsPerSecond: 5_000_000});
      const chunks: Blob[] = [];
      let bytes = 0, raf = 0, start = 0, next = 0, lastState = 0, done = false, problem: Error | undefined;
      let timeout: ReturnType<typeof setTimeout> | undefined;
      const finish = () => {
        if (done) return; done = true;
        cancelAnimationFrame(raf); clearTimeout(timeout);
        options.signal.removeEventListener('abort', abort); document.removeEventListener('visibilitychange', hidden);
        recorder.ondataavailable = recorder.onstop = recorder.onerror = recorder.onstart = null;
        problem ? reject(problem) : bytes ? resolve(new Blob(chunks, {type: recorder.mimeType || support.mimeType})) : reject(new Error('The browser produced no video. Try GIF or another browser.'));
      };
      const stop = (error?: Error) => {
        problem ??= error;
        cancelAnimationFrame(raf);
        if (recorder.state === 'inactive') finish();
        else {recorder.stop(); timeout ??= setTimeout(() => {problem ??= new Error('Video finalization timed out.'); finish();}, 5_000);}
      };
      const abort = () => stop(canceled());
      const hidden = () => {if (document.hidden) stop(new Error('Video export stopped because the tab was hidden. Keep it visible and retry.'));};
      const tick = (now: number) => {
        if (done || recorder.state !== 'recording') return;
        const elapsed = (now - start) / 1000;
        if (elapsed >= duration) {stop(lastState === starts.length - 1 ? undefined : new Error('The browser missed the final visual state. Lower playback speed or choose GIF.')); return;}
        if (elapsed >= next) {
          try {
            lastState = videoState(starts, elapsed, lastState); render(elapsed * options.speed);
            next = (Math.floor(elapsed * FPS) + 1) / FPS; options.onProgress(.98 * elapsed / duration);
          }
          catch (error) {stop(error as Error); return;}
        }
        if (!done && recorder.state === 'recording') raf = requestAnimationFrame(tick);
      };
      recorder.ondataavailable = event => {bytes += event.data.size; chunks.push(event.data); if (bytes > EXPORT_LIMITS.video.bytes) stop(new Error('Video reached 128 MiB. Increase speed and retry.'));};
      recorder.onerror = () => stop(new Error(`${support.label} recording failed. Try GIF or another browser.`));
      recorder.onstop = finish;
      recorder.onstart = () => {start = performance.now(); raf = requestAnimationFrame(tick);};
      options.signal.addEventListener('abort', abort, {once: true}); document.addEventListener('visibilitychange', hidden);
      try {check(options.signal); render(0); recorder.start(1000);} catch (error) {problem = error as Error; finish();}
    });
  } finally {stream.getTracks().forEach(track => track.stop());}
}

let active = false;
export async function exportScenario(options: ExportOptions): Promise<{blob: Blob; extension: string; mimeType: string}> {
  if (active) throw new Error('An export is already running. Finish or cancel it first.');
  if (options.format !== 'gif' && options.format !== 'video') throw new Error('Choose GIF or video.');
  exportDuration(options.drawing, options.speed); check(options.signal); active = true;
  const canvas = document.createElement('canvas'); canvas.width = WIDTH; canvas.height = HEIGHT;
  try {
    const context = canvas.getContext('2d', {alpha: false, willReadFrequently: options.format === 'gif'});
    if (!context) throw new Error('Cannot create the export canvas.');
    const scale = Math.min(WIDTH / options.drawing.width, HEIGHT / options.drawing.height);
    const render = (time: number) => {
      context.resetTransform(); context.fillStyle = '#000000'; context.fillRect(0, 0, WIDTH, HEIGHT);
      context.translate((WIDTH - options.drawing.width * scale) / 2, (HEIGHT - options.drawing.height * scale) / 2); context.scale(scale, scale);
      options.paint(context, options.drawing, time, options.theme, options.dark);
    };
    options.onProgress(0); await abortable(document.fonts.ready, options.signal);
    const blob = options.format === 'gif' ? await gif(context, render, options) : await video(canvas, render, options);
    check(options.signal); options.onProgress(1);
    return {blob, extension: options.format === 'gif' ? 'gif' : blob.type.startsWith('video/mp4') ? 'mp4' : 'webm', mimeType: blob.type};
  } finally {canvas.width = canvas.height = 0; active = false;}
}
