/**
 * WORKLOAD §2 perf sanity: sustained group-commit writes/sec THROUGH THE PIPELINE (not straight at
 * the adapter — the 5ms batch window is half of what makes the number what it is), memory vs
 * postgres. Not a pass/fail gate: a baseline for Phase 5's load simulator.
 *
 *   node --import tsx scripts/bench-writes.ts [seconds] [inflight]
 */
import { randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { dropSchema, PG_URL, uniqueSchema } from '../harness/pg.ts';
import { allowAll } from '../src/pipeline/rules.ts';
import { WritePipeline } from '../src/pipeline/write.ts';
import { DEFAULT_LIMITS } from '../src/protocol/limits.ts';
import type { ServerFrame } from '../src/protocol/frames.ts';
import type { StorageAdapter } from '../src/storage/adapter.ts';
import { MemoryStorage } from '../src/storage/memory.ts';
import { PostgresStorage } from '../src/storage/postgres.ts';

const SECONDS = Number(process.argv[2] ?? 3);
const INFLIGHT = Number(process.argv[3] ?? 200);

interface Result {
  acks: number;
  perSec: number;
  meanMs: number;
  p99Ms: number;
}

async function bench(storage: StorageAdapter): Promise<Result> {
  // WritePipeline unrefs its batch timer — correct under a gateway, whose socket holds the loop
  // open, but in a bare script an unref'd timer is the only handle left and Node exits before the
  // first batch drains. One ref'd timer stands in for the socket.
  const keepAlive = setInterval(() => undefined, 1000);
  const pipeline = new WritePipeline(storage, allowAll, DEFAULT_LIMITS);
  const latencies: number[] = [];

  const one = (i: number): Promise<void> =>
    new Promise((resolve) => {
      const started = performance.now();
      pipeline.submit({
        frame: { type: 'put', writeId: randomUUID(), path: `bench/${i % 1000}/v`, value: i },
        userId: 'u_bench',
        reply: (f: ServerFrame) => {
          if (f.type !== 'ack') throw new Error(`unexpected reply: ${f.type}`);
          latencies.push(performance.now() - started);
          resolve();
        },
      });
    });

  // Warm the connection pool and JIT before the clock starts; otherwise the first second measures
  // TCP handshakes (the Gate B lesson, in bench form).
  await Promise.all(Array.from({ length: INFLIGHT }, (_, i) => one(i)));
  latencies.length = 0;

  let n = INFLIGHT;
  const t0 = performance.now();
  const deadline = t0 + SECONDS * 1000;
  // Each worker keeps exactly one write in flight, so the batch window always has something to
  // gather and the number is a sustained rate rather than one enormous batch.
  await Promise.all(
    Array.from({ length: INFLIGHT }, async () => {
      while (performance.now() < deadline) await one(n++);
    }),
  );
  const elapsed = (performance.now() - t0) / 1000;

  clearInterval(keepAlive);
  const sorted = [...latencies].sort((a, b) => a - b);
  return {
    acks: latencies.length,
    perSec: latencies.length / elapsed,
    meanMs: sorted.reduce((a, b) => a + b, 0) / sorted.length,
    p99Ms: sorted[Math.floor(sorted.length * 0.99)] ?? 0,
  };
}

const row = (name: string, r: Result): string =>
  `${name.padEnd(10)} ${Math.round(r.perSec).toString().padStart(8)} w/s   ${r.acks.toString().padStart(7)} acks   mean ${r.meanMs.toFixed(2)}ms   p99 ${r.p99Ms.toFixed(2)}ms`;

console.log(`sustained put/s through WritePipeline — ${SECONDS}s, ${INFLIGHT} in flight, batch window ${DEFAULT_LIMITS.GROUP_COMMIT_MS}ms\n`);
console.log(row('memory', await bench(new MemoryStorage(DEFAULT_LIMITS))));

const schema = uniqueSchema('bench');
const pg = new PostgresStorage({ url: PG_URL, limits: DEFAULT_LIMITS, schema });
try {
  console.log(row('postgres', await bench(pg)));
} finally {
  await pg.close();
  await dropSchema(schema);
}
