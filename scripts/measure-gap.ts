/**
 * WORKLOAD §5.15 — measure the INTER-TRANSACTION GAP and event-loop lag as fanout load rises.
 *
 * The claim under test: at ρ≈0.96 the lock cycle has only ~0.32ms of slack, and delivery work on
 * the same single-threaded event loop is what eats it — so the gap grows with subscriber count and
 * the served rate falls below demand, which is the gradient. Refutation: a flat gap.
 *
 * Two things this harness gets right on purpose, because getting either wrong measures nothing:
 *
 *  1. CLIENTS RUN ELSEWHERE. Subscribers in this process would contend for this event loop, and the
 *     measurement would be of the harness. loadsim.ts drives the load from forked processes.
 *
 *  2. THE HOLD IS SIMULATED AT THE PRODUCTION FIGURE. MemoryStorage commits in microseconds, which
 *     is a different regime entirely: with a ~0µs hold there is no cycle to steal slack from. The
 *     wrapper below holds for HOLD_MS (default 7.74, the figure derived from ρ = λS on 2026-09-03)
 *     so the cycle matches the system being explained. The hold is a `setTimeout` await, NOT a busy
 *     loop: a spin would occupy the very event loop whose contention is the subject.
 *
 * The gap is only meaningful while work is always queued — an idle gap is just waiting for a write.
 * So the run must SATURATE, and the report prints the achieved commit rate against the ceiling
 * 1/(hold+gap) as the check: if they agree, the run was saturated and the gaps are real.
 */
import { monitorEventLoopDelay } from 'node:perf_hooks';
import { startGateway } from '../src/gateway/server.ts';
import { MemoryStorage } from '../src/storage/memory.ts';
import { DEFAULT_LIMITS } from '../src/protocol/limits.ts';
import type {
  StorageAdapter,
  GroupWrite,
  AckResult,
  CasWrite,
  CasResult,
} from '../src/storage/adapter.ts';

const PORT = Number(process.env['PORT'] ?? 8099);
const HOLD_MS = Number(process.env['HOLD_MS'] ?? 7.74);

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

interface Sample {
  gapMs: number;
  batch: number;
  /** Measured, not nominal — see the note in #timed. */
  holdMs?: number;
  /** The commit call itself. NOT dead time — see the note in #timed. */
  runMs?: number;
}

class GapStorage implements StorageAdapter {
  #lastEnd = 0;
  readonly samples: Sample[] = [];
  commits = 0;
  writes = 0;

  constructor(private readonly inner: MemoryStorage) {}

  async #timed<T>(batch: number, run: () => Promise<T>): Promise<T> {
    const start = performance.now();
    // The first commit has no predecessor, so it has no gap to report.
    if (this.#lastEnd !== 0) this.samples.push({ gapMs: start - this.#lastEnd, batch });
    const out = await run();
    // The third bucket, and the one that hid the delivery work. MemoryStorage calls #notify()
    // SYNCHRONOUSLY inside commitGroup, so onCommit -> Dispatcher#kick -> pump() starts here; #drain
    // then awaits readOplogSince before its first stream.append, so route() and the per-connection
    // sends land in the microtask turn between this await and the line below. Neither the gap nor
    // the hold covers that window. Leaving it untimed HIDES delivery-side cost and OVERSTATES the
    // ceiling, so a genuinely saturated run reads as unsaturated and gets thrown away.
    // The sleep stands in for the counter lock held across the nodes work. It is TIMED, not
    // assumed: setTimeout fires LATE under event-loop lag, so the very CPU tax the gradient
    // hypothesis is about lands inside this window. Charging it to a nominal constant would hide
    // it, and a genuinely saturated run would then read as unsaturated.
    const heldFrom = performance.now();
    await sleep(HOLD_MS);
    this.#lastEnd = performance.now();
    const last = this.samples[this.samples.length - 1];
    if (last) {
      last.holdMs = this.#lastEnd - heldFrom;
      last.runMs = heldFrom - start;
    }
    this.commits += 1;
    this.writes += batch;
    return out;
  }

  commitGroup(w: GroupWrite[]): Promise<AckResult[]> {
    return this.#timed(w.length, () => this.inner.commitGroup(w));
  }
  commitCas(w: CasWrite): Promise<CasResult> {
    return this.#timed(1, () => this.inner.commitCas(w));
  }

  head(): Promise<number> { return this.inner.head(); }
  epoch(): Promise<number> { return this.inner.epoch(); }
  prunedThroughRev(): Promise<number> { return this.inner.prunedThroughRev(); }
  readSnapshot(p: string) { return this.inner.readSnapshot(p); }
  readCatchup(p: string, s: number, l: number) { return this.inner.readCatchup(p, s, l); }
  readOplogSince(a: number, l: number) { return this.inner.readOplogSince(a, l); }
  topNodes(): Promise<string[]> { return this.inner.topNodes(); }
  onCommit(cb: () => void): () => void { return this.inner.onCommit(cb); }
}

const pct = (xs: number[], p: number): number =>
  xs.length === 0 ? 0 : [...xs].sort((a, b) => a - b)[Math.min(xs.length - 1, Math.floor(xs.length * p))]!;

/**
 * What `sleep(HOLD_MS)` ACTUALLY costs on this machine — measured at boot, TWICE, because it is not
 * one number.
 *
 * Two separate reasons the nominal constant is not the floor:
 *
 *  1. setTimeout takes INTEGER milliseconds. setTimeout(7.74), setTimeout(7) and setTimeout(7.99)
 *     are the same call; setTimeout(8) is a different one. The fraction is discarded, so the 7.74ms
 *     figure this harness exists to reproduce has never once been held — the rig holds 7. Every
 *     `measured hold - HOLD_MS` subtraction is arithmetic against a value that did not occur.
 *
 *  2. The floor MOVES WITH LOAD, in the direction that matters. Idle, the loop sleeps in kqueue and
 *     pays wake-up latency getting back to the timers phase; busy, it is already passing through
 *     that phase and catches the deadline sooner. Measured here: idle 7.951ms, busy 7.075ms. So a
 *     floor sampled at boot on an idle loop is sampled under the one condition that never holds
 *     during a run, and subtracting it yields NEGATIVE loop-blocking (7.09 - 7.94 = -0.85ms at
 *     500 conns). That is the same error as the nominal, one layer down: a constant standing in for
 *     something that varies with exactly the load under study.
 *
 * So: a bracket, not a constant. Excess is measured against the BUSY floor, which is the condition
 * a loaded run is actually in, and the idle floor is reported next to it so the load-dependence
 * stays visible instead of being buried in one more number.
 *
 * The spin below is the one place a busy-wait is correct in this file: it runs at BOOT, before the
 * gateway accepts a connection, so it cannot contend with the delivery work under study — which is
 * why the hold itself is still a setTimeout and not a spin (see #timed).
 */
const measureHoldFloor = async (churn: boolean, n = 200): Promise<number> => {
  let spinning = churn;
  const spin = (): void => {
    if (!spinning) return;
    const end = performance.now() + 0.4;
    while (performance.now() < end);
    setImmediate(spin);
  };
  if (churn) setImmediate(spin);
  const xs: number[] = [];
  for (let i = 0; i < n; i++) {
    const t = performance.now();
    await sleep(HOLD_MS);
    xs.push(performance.now() - t);
  }
  spinning = false;
  return pct(xs, 0.5);
};
const holdFloorIdleMs = await measureHoldFloor(false);
const holdFloorBusyMs = await measureHoldFloor(true);

const storage = new GapStorage(new MemoryStorage(DEFAULT_LIMITS));
// Anchors the accounting window. Reset by SIGUSR2 with the counters, so `accounted` below is
// against the window the driver actually measured, not since boot.
let windowFrom = performance.now();
const loop = monitorEventLoopDelay({ resolution: 1 });
loop.enable();

const gw = await startGateway({ port: PORT, storage, limits: DEFAULT_LIMITS });
process.stdout.write(
  `gap-gw listening ${gw.port} hold=${HOLD_MS}ms ` +
    `(floor idle ${holdFloorIdleMs.toFixed(3)}ms / busy ${holdFloorBusyMs.toFixed(3)}ms)\n`,
);

// A report on SIGUSR2 rather than on a timer: the driver decides when the window is over, so the
// numbers cover exactly the load phase and not the ramp-up before it.
const report = (): void => {
  const s = storage.samples;
  const gaps = s.map((x) => x.gapMs);
  const batches = s.map((x) => x.batch);
  const holds = s.map((x) => x.holdMs).filter((x): x is number => x !== undefined);
  const runs = s.map((x) => x.runMs).filter((x): x is number => x !== undefined);
  const gapP50 = pct(gaps, 0.5);
  const holdP50 = pct(holds, 0.5);
  const runP50 = pct(runs, 0.5);
  const meanBatch = batches.reduce((a, b) => a + b, 0) / Math.max(1, batches.length);
  // From the MEASURED cycle. The mechanism predicts the cycle grows with fanout load; which of the
  // THREE buckets absorbs it is an artifact of where the work lands, so the ceiling must not depend
  // on that split. runMs is the commit call itself, and it carries the synchronous fanout the
  // dispatcher kicks off — omitting it overstated the ceiling and made saturated runs read idle.
  // Guarded: with no samples this is 1000/0 = Infinity, which JSON.stringify renders as `null` —
  // a report that looks like a measurement and is not one.
  const sumGap = gaps.reduce((a, b) => a + b, 0);
  const sumRun = runs.reduce((a, b) => a + b, 0);
  const sumHold = holds.reduce((a, b) => a + b, 0);
  const windowMs = performance.now() - windowFrom;
  const cycle = holdP50 + runP50 + gapP50;
  const ceilingPerSec = cycle > 0 ? 1000 / cycle : null;
  process.stdout.write(
    JSON.stringify({
      commits: storage.commits,
      writes: storage.writes,
      meanBatch: +meanBatch.toFixed(2),
      gapMs: {
        p50: +gapP50.toFixed(3),
        p90: +pct(gaps, 0.9).toFixed(3),
        p99: +pct(gaps, 0.99).toFixed(3),
        max: +pct(gaps, 1).toFixed(3), // spread on ~116k samples is a stack overflow, not a slow path
      },
      // The accounting identity, and the reason it REPLACES achieved-vs-ceiling as the check that
      // a window is worth reading. The three buckets TILE: gap_n = start_n - lastEnd_(n-1),
      // run_n = heldFrom_n - start_n, hold_n = lastEnd_n - heldFrom_n, so summing them telescopes
      // to lastEnd_last - lastEnd_first. There is no fourth place for wall time to hide between
      // the first and last commit of a window, and `accountedPct` well under 100 means the window
      // is mostly ramp or idle tail, NOT that time escaped the cycle.
      // Percentiles cannot do this arithmetic: commits x p50-cycle understates a heavy tail badly
      // (a 24-sample window here carried a 3330ms run max against a 2.3ms run p50), so the sums
      // are what the accounting must be read from.
      // The old check assumed a fixed batch size. meanBatch moved 18 -> 2202 across configs, so
      // commits/s collapses while the system is maximally saturated and the ceiling rule would
      // reject the most saturated window in the set.
      windowMs: +windowMs.toFixed(1),
      // Subtract THIS, not HOLD_MS, to get loop-blocking inside the hold window.
      // A bracket, not a constant — the floor moves with load. Excess is against the BUSY floor;
      // the idle one is here so the spread stays visible rather than becoming another phantom.
      holdFloorMs: { idle: +holdFloorIdleMs.toFixed(3), busy: +holdFloorBusyMs.toFixed(3) },
      holdExcessMs: +(holdP50 - holdFloorBusyMs).toFixed(3),
      sumMs: {
        gap: +sumGap.toFixed(1),
        run: +sumRun.toFixed(1),
        hold: +sumHold.toFixed(1),
        total: +(sumGap + sumRun + sumHold).toFixed(1),
      },
      accountedPct: windowMs > 0 ? +(((sumGap + sumRun + sumHold) / windowMs) * 100).toFixed(1) : null,
      runMs: {
        p50: +runP50.toFixed(3),
        p99: +pct(runs, 0.99).toFixed(3),
        max: +pct(runs, 1).toFixed(3),
      },
      holdMs: {
        p50: +holdP50.toFixed(3),
        p99: +pct(holds, 0.99).toFixed(3),
        max: +pct(holds, 1).toFixed(3),
      },
      loopLagMs: {
        mean: +(loop.mean / 1e6).toFixed(3),
        p99: +(loop.percentile(99) / 1e6).toFixed(3),
        max: +(loop.max / 1e6).toFixed(3),
      },
      cycleMs: +cycle.toFixed(3),
      ceilingCommitsPerSec: ceilingPerSec === null ? 'no samples' : +ceilingPerSec.toFixed(1),
      // The saturation check: if the achieved rate sits at the ceiling the run was saturated and
      // the gaps are real overhead. Well below it means the queue drained and some gaps are just
      // idle waiting — reject that window rather than reading it.
      achievedCommitsPerSec: 'divide commits by the window you timed',
    }) + '\n',
  );
};

process.on('SIGUSR2', () => {
  report();
  storage.samples.length = 0;
  storage.commits = 0;
  storage.writes = 0;
  windowFrom = performance.now();
  loop.reset();
});
process.on('SIGTERM', () => {
  gw.close();
  process.exit(0);
});
