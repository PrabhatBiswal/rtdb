/**
 * Phase 2's design question, measured: is the right quota unit writes/second, or lock
 * acquisitions/second? Firebase quotas writes. Our measured ceiling is ~129 LOCK ACQUISITIONS/s.
 * If those diverge by workload SHAPE, quotaing writes prices the wrong thing.
 *
 *   node --import tsx scripts/quota-unit.ts
 *
 * Storage is a STUB on purpose: the question is how many transactions the pipeline STARTS, not how
 * long one takes, and a stub keeps the arithmetic exact instead of sampled.
 *
 * Cases 1-7 share ONE pipeline, which is what the gateway had before Phase 3. Case 8 is the shape
 * it has now — a pipeline per database — and it answers case 5's prediction that total acquisitions
 * would RISE once cross-tenant batching went away. Case 9 turns §5.23's quota on and off over the
 * same two tenants and counts who gets refused. Takes about 15s, most of it case 9's real seconds.
 */
import { randomUUID } from 'node:crypto';
import { AcquisitionQuota, WritePipeline } from '../src/pipeline/write.ts';
import { allowAll } from '../src/pipeline/rules.ts';
import { DEFAULT_LIMITS } from '../src/protocol/limits.ts';
import { countAcquisition, lockAcquisitions } from '../src/gateway/metrics.ts';
import type { StorageAdapter, GroupWrite, AckResult, CasWrite, CasResult } from '../src/storage/adapter.ts';

let rev = 0;
const acq = { group: 0, cas: 0 };
const batchSizes: number[] = [];

/** Counts LOCK ACQUISITIONS: one per commitGroup (whatever the batch), one per solo CAS (§4). */
const storage = {
  commitGroup: async (w: GroupWrite[]): Promise<AckResult[]> => {
    acq.group++;
    batchSizes.push(w.length);
    return w.map((x) => ({ writeId: x.writeId, rev: ++rev, duplicate: false }));
  },
  commitCas: async (_w: CasWrite): Promise<CasResult> => {
    acq.cas++;
    return { ok: true, rev: ++rev, duplicate: false };
  },
} as unknown as StorageAdapter;

/**
 * §5.23 Gate A: the same `onAcquire` the gateway wires, so this script MEASURES the counter rather
 * than a parallel count of its own. The `metered` column below is `rtdb_lock_acquisitions_total`
 * and must equal `acquisitions`, which is what the stub saw — two independent sides of one claim,
 * and the reason this file is the meter's tooth. `test/integration/quota-meter.test.ts` asserts the
 * same equality through a real gateway, where it can fail a battery instead of a reader's eye.
 */
const p = new WritePipeline(
  storage, allowAll, DEFAULT_LIMITS, undefined, undefined, () => countAcquisition('quota-unit'),
);

const metered = async (): Promise<number> => {
  const m = await lockAcquisitions.get();
  return m.values.find((v) => v.labels['db'] === 'quota-unit')?.value ?? 0;
};
let meterBefore = 0;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const put = (path: string) => ({
  frame: { type: 'put' as const, writeId: randomUUID(), path, value: 1 },
  userId: 'u', reply: () => undefined,
});
const cas = (path: string) => ({
  frame: { type: 'cas' as const, writeId: randomUUID(), path, expectedRev: rev, value: 1 },
  userId: 'u', reply: () => undefined,
});

const reset = async () => {
  acq.group = 0; acq.cas = 0; batchSizes.length = 0;
  meterBefore = await metered();
};
/**
 * `writes` is DERIVED from what actually reached storage, never passed in. Case 5 used to pass a
 * hand-written 550 while its workload submitted 1050, and the wrong ratio sat in the same printed
 * line as the right one — `writes/acq 11.00` beside `mean batch 21.00`, two columns that cannot
 * both be true — for a whole phase. Nothing here takes a count on trust any more.
 */
const report = async (label: string) => {
  const a = acq.group + acq.cas;
  const writes = batchSizes.reduce((x, y) => x + y, 0) + acq.cas;
  const mean = batchSizes.length ? (writes - acq.cas) / batchSizes.length : 0;
  const meter = (await metered()) - meterBefore;
  console.log(
    `${label.padEnd(42)} writes ${String(writes).padStart(5)}  acquisitions ${String(a).padStart(5)}` +
    `  writes/acq ${(writes / a).toFixed(2).padStart(6)}  mean batch ${mean.toFixed(2)}` +
    `  metered ${String(meter).padStart(5)}${meter === a ? '' : '  <-- METER DISAGREES'}`,
  );
};

// 1. One burst: everything arrives inside one 5ms window.
await reset();
for (let i = 0; i < 500; i++) p.submit(put(`db/a/${i}`));
await p.flush(); await sleep(20);
await report('BURST 500 writes at once');

// 2. Trickle: one write every 10ms, i.e. slower than the 5ms window.
await reset();
for (let i = 0; i < 60; i++) { p.submit(put(`db/b/${i}`)); await sleep(10); }
await p.flush(); await sleep(20);
await report('TRICKLE 60 writes, one per 10ms');

// 3. Steady inside the window: one write every 1ms.
await reset();
for (let i = 0; i < 300; i++) { p.submit(put(`db/c/${i}`)); await sleep(1); }
await p.flush(); await sleep(20);
await report('STEADY 300 writes, one per 1ms');

// 4. The same burst, but 5% of it is CAS - loadsim's own default mix.
await reset();
for (let i = 0; i < 500; i++) p.submit(i % 20 === 0 ? cas(`db/d/${i}`) : put(`db/d/${i}`));
await p.flush(); await sleep(50);
await report('BURST 500 with 5% CAS');

// 5. TWO tenants sharing ONE pipeline: one bursty, one trickling. Today #pending is per GATEWAY.
//    1000 bursty writes, not 500 — the label said 500 and the hand-written count in `report` said
//    550 for a whole phase, while `mean batch` next to it said otherwise. Both derive now.
await reset();
const trickle = (async () => {
  for (let i = 0; i < 50; i++) { p.submit(put(`slow/x/${i}`)); await sleep(10); }
})();
const bursty = (async () => {
  for (let r = 0; r < 10; r++) {
    for (let i = 0; i < 100; i++) p.submit(put(`fast/y/${r}-${i}`));
    await sleep(50);
  }
})();
await Promise.all([trickle, bursty]);
await p.flush(); await sleep(20);
await report('SHARED pipeline: 1000 bursty + 50 trickle');

// 6. And the interference: does ONE tenant's CAS truncate ANOTHER tenant's batch window?
await reset();
const writer = (async () => {
  for (let i = 0; i < 200; i++) { p.submit(put(`victim/${i}`)); await sleep(1); }
})();
const casser = (async () => {
  for (let i = 0; i < 40; i++) { p.submit(cas(`attacker/${i}`)); await sleep(5); }
})();
await Promise.all([writer, casser]);
await p.flush(); await sleep(50);
await report('SHARED: 200 steady puts + 40 CAS alongside');

// 6b. control - the same 200 puts with NO cas alongside.
await reset();
for (let i = 0; i < 200; i++) { p.submit(put(`control/${i}`)); await sleep(1); }
await p.flush(); await sleep(20);
await report('CONTROL: the same 200 steady puts alone');

// ===========================================================================================
// 8. ONE PIPELINE PER DATABASE — the shape Phase 3 actually shipped, and case 5's prediction
//    checked rather than assumed. Case 5 above says a trickling tenant's writes ride the bursty
//    tenant's batches for free; per-database pipelines end that, so the SHARD pays more in total
//    while each tenant pays exactly its own. That difference is what makes a per-database bill
//    possible at all — before it, one tenant's batch WAS the other's.
// ===========================================================================================

/** One tenant's own store, counting only its own acquisitions. */
function tenantStore(): { storage: StorageAdapter; acq: () => number; batches: number[] } {
  let own = 0;
  let ownRev = 0;
  const batches: number[] = [];
  const storage = {
    commitGroup: async (w: GroupWrite[]): Promise<AckResult[]> => {
      own++; batches.push(w.length);
      return w.map((x) => ({ writeId: x.writeId, rev: ++ownRev, duplicate: false }));
    },
    commitCas: async (_w: CasWrite): Promise<CasResult> => {
      own++; return { ok: true, rev: ++ownRev, duplicate: false };
    },
  } as unknown as StorageAdapter;
  return { storage, acq: () => own, batches };
}

const submitTo = (p: WritePipeline, path: string): void =>
  p.submit({ frame: { type: 'put', writeId: randomUUID(), path, value: 1 }, userId: 'u', reply: () => undefined });

const row = (label: string, acq: number, ms: number, batches: number[], rate?: number): void => {
  const writes = batches.reduce((x, y) => x + y, 0);
  const mean = batches.length ? writes / batches.length : 0;
  console.log(
    `${label.padEnd(38)} writes ${String(writes).padStart(6)}  acq ${String(acq).padStart(4)}` +
    `  writes/acq ${(writes / (acq || 1)).toFixed(2).padStart(7)}  acq/s ${(acq / (ms / 1000)).toFixed(1).padStart(6)}` +
    `  mean batch ${mean.toFixed(2).padStart(7)}${rate === undefined ? '' : `  RATE ${String(rate).padStart(5)}`}`,
  );
};

console.log('');
{
  const shared = tenantStore();
  const p8 = new WritePipeline(shared.storage, allowAll, DEFAULT_LIMITS);
  const t0 = Date.now();
  await Promise.all([
    (async () => { for (let i = 0; i < 50; i++) { submitTo(p8, `slow/x/${i}`); await sleep(10); } })(),
    (async () => { for (let r = 0; r < 10; r++) { for (let i = 0; i < 100; i++) submitTo(p8, `fast/y/${r}-${i}`); await sleep(50); } })(),
  ]);
  await p8.flush(); await sleep(20);
  row('SHARED (pre-Phase 3): both tenants', shared.acq(), Date.now() - t0 - 20, shared.batches);
}
{
  const slow = tenantStore(), fast = tenantStore();
  const ps = new WritePipeline(slow.storage, allowAll, DEFAULT_LIMITS);
  const pf = new WritePipeline(fast.storage, allowAll, DEFAULT_LIMITS);
  const t0 = Date.now();
  await Promise.all([
    (async () => { for (let i = 0; i < 50; i++) { submitTo(ps, `slow/x/${i}`); await sleep(10); } })(),
    (async () => { for (let r = 0; r < 10; r++) { for (let i = 0; i < 100; i++) submitTo(pf, `fast/y/${r}-${i}`); await sleep(50); } })(),
  ]);
  await ps.flush(); await pf.flush(); await sleep(20);
  const ms = Date.now() - t0 - 20;
  row('  tenant slow (trickle 1/10ms)', slow.acq(), ms, slow.batches);
  row('  tenant fast (bursty 100/50ms)', fast.acq(), ms, fast.batches);
  row('PER-TENANT (Phase 3): both', slow.acq() + fast.acq(), ms, [...slow.batches, ...fast.batches]);
}

// ===========================================================================================
// 9. THE QUOTA, OFF then ON, over the same two tenants at the SHIPPED default (§5.23: 64/s,
//    burst 128). The admission check is the two lines `server.ts` runs — `allows()` before the
//    submit, `charge()` at commit — because this file measures the unit and `quota.test.ts`
//    tests the gateway that enforces it. Long enough arms that the bucket is actually reached:
//    at 100 writes/s the trickling tenant spends 100 acq/s against a 64 acq/s allowance.
// ===========================================================================================

const ARM_MS = 5000;

/**
 * One tenant under the quota: its own store, its own pipeline, its own bucket. `admit` is exactly
 * what `server.ts` does — `allows()` before the write is submitted, `charge()` when the commit
 * happens — so the refusals counted here are the ones a client would have been sent.
 */
function tenantUnderQuota(enforced: boolean): {
  submit: (path: string) => void;
  flush: () => Promise<void>;
  acq: () => number;
  rate: () => number;
  batches: number[];
} {
  const store = tenantStore();
  const bucket = new AcquisitionQuota(DEFAULT_LIMITS.QUOTA_ACQ_PER_SEC, DEFAULT_LIMITS.QUOTA_ACQ_BURST);
  let refused = 0;
  const p = new WritePipeline(store.storage, allowAll, DEFAULT_LIMITS, undefined, undefined, () => bucket.charge());
  return {
    submit: (path) => {
      if (enforced && !bucket.allows()) { refused++; return; }
      submitTo(p, path);
    },
    flush: async () => { await p.flush(); },
    acq: store.acq,
    rate: () => refused,
    batches: store.batches,
  };
}

async function arm(enforced: boolean): Promise<void> {
  const slow = tenantUnderQuota(enforced);
  const fast = tenantUnderQuota(enforced);
  const until = Date.now() + ARM_MS;
  const t0 = Date.now();
  await Promise.all([
    // A trickle: one write every 10ms, so every write is its own acquisition — 100 acq/s.
    (async () => { let i = 0; while (Date.now() < until) { slow.submit(`slow/x/${i++}`); await sleep(10); } })(),
    // A burst: 100 writes every 50ms, so a hundred writes cost ONE acquisition — 20 acq/s.
    (async () => {
      let r = 0;
      while (Date.now() < until) { for (let i = 0; i < 100; i++) fast.submit(`fast/y/${r}-${i}`); r++; await sleep(50); }
    })(),
  ]);
  await slow.flush(); await fast.flush(); await sleep(20);
  const ms = Date.now() - t0 - 20;
  const tag = enforced ? 'ON ' : 'OFF';
  row(`QUOTA ${tag}  tenant slow (trickle)`, slow.acq(), ms, slow.batches, slow.rate());
  row(`QUOTA ${tag}  tenant fast (bursty)`, fast.acq(), ms, fast.batches, fast.rate());
}

console.log(`\n(quota ${DEFAULT_LIMITS.QUOTA_ACQ_PER_SEC}/s, burst ${DEFAULT_LIMITS.QUOTA_ACQ_BURST}; ${ARM_MS / 1000}s per arm)`);
await arm(false);
await arm(true);
