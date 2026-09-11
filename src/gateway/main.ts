// Load-bearing and it does not look it: this registers the non-exiting unhandled-rejection
// backstop as a side effect and exports nothing. See crash-guard.ts. It sits with the imports on
// purpose — an import is evaluated before this module's body, so the guard is armed before
// `startGateway` is awaited below; the previous inline version registered AFTER that await and so
// could not catch a rejection thrown during startup.
import './crash-guard.ts';
import { connectRedis } from '../fanout/redis.ts';
import { startAdminServer } from './metrics.ts';
import { startGateway } from './server.ts';
import { makeLimits, type Limits } from '../protocol/limits.ts';
import type { StorageAdapter } from '../storage/adapter.ts';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import type { Rules } from '../pipeline/rules.ts';
import { MemoryStorage } from '../storage/memory.ts';
import { openSharedTenancy, PostgresStorage } from '../storage/postgres.ts';
import { bindPoolWaiting } from './metrics.ts';
import type { StorageAdapter as Adapter } from '../storage/adapter.ts';

/**
 * Standalone gateway process, so the chaos runner can SIGKILL it and start it again
 * (WORKLOAD §5). Config comes from the environment because that is all a spawned process gets.
 * `RTDB_LIMITS` is a JSON patch over the §9 defaults.
 */
const port = Number(process.env['RTDB_PORT'] ?? 0);
/**
 * §5.23: the per-database acquisition quota, as its own knobs rather than only inside `RTDB_LIMITS`.
 * These two are deploy-review numbers — the shard's ceiling is ~129 acq/s and the default takes
 * half of it — so an operator changes them the way they change the pool size, not by hand-writing a
 * JSON patch. `RTDB_LIMITS` still wins if it names them, because it is the more specific instrument.
 */
const quotaPerSec = Number(process.env['RTDB_QUOTA_ACQ_PER_SEC'] ?? 0);
const quotaBurst = Number(process.env['RTDB_QUOTA_ACQ_BURST'] ?? 0);
const limits: Limits = makeLimits({
  ...(quotaPerSec > 0 ? { QUOTA_ACQ_PER_SEC: quotaPerSec } : {}),
  ...(quotaBurst > 0 ? { QUOTA_ACQ_BURST: quotaBurst } : {}),
  ...(process.env['RTDB_LIMITS'] ? (JSON.parse(process.env['RTDB_LIMITS']) as Partial<Limits>) : {}),
});

/**
 * §5.23 faisla 5: how many gateways serve this shard. Each takes `1/G` of every database's quota,
 * because the bucket is per gateway and a shared one would put a Redis round trip on the write path.
 * Production runs 2; unset is 1, which is every test and every single-gateway deployment.
 */
const gatewayCount = Number(process.env['RTDB_GATEWAY_COUNT'] ?? 1);

/**
 * §2 wiring: `RTDB_STORAGE=memory|postgres`, memory by default — nothing that does not ask for
 * Postgres changes behaviour. `RTDB_PG_SCHEMA` exists so several gateways (chaos scenarios, CI) can
 * share one database without sharing a shard.
 */
const storageKind = process.env['RTDB_STORAGE'] ?? 'memory';
const defaultSchema = process.env['RTDB_PG_SCHEMA'] ?? 'public';
// §5.22 Gate A. Unset is the right answer for a single shard — the default is a real, distinct
// schema, not the tenant's — and this exists so two shards can share one Postgres database the same
// way `RTDB_PG_SCHEMA` already lets them.
const controlSchema = process.env['RTDB_CONTROL_SCHEMA'];
const control = controlSchema ? { controlSchema } : {};

function pgUrl(): string {
  const url = process.env['RTDB_PG_URL'];
  if (!url) throw new Error('RTDB_STORAGE=postgres requires RTDB_PG_URL');
  return url;
}

function storageFromEnv(): StorageAdapter {
  if (storageKind === 'memory') return new MemoryStorage(limits, process.env['RTDB_PERSIST']);
  if (storageKind !== 'postgres') {
    throw new Error(`RTDB_STORAGE must be "memory" or "postgres", got "${storageKind}"`);
  }
  // Deploy-review knob (WORKLOAD §2): connections per gateway to RDS. Unset keeps WP4's 10.
  const poolMax = Number(process.env['RTDB_PG_POOL'] ?? 0);
  return new PostgresStorage({
    url: pgUrl(),
    limits,
    schema: defaultSchema,
    ...control,
    ...(poolMax > 0 ? { poolMax } : {}),
  });
}

/**
 * §5.22 Gate D: one database per Postgres SCHEMA, all of them sharing this process's pool and its
 * one LISTEN connection — which is the whole of Gates B and C paying off. Off unless asked for:
 * unset, this gateway is the single-database deployment it has always been.
 *
 * `RTDB_MULTI_TENANT=1` and Postgres only. Memory storage has no schemas to put tenants in, and a
 * per-tenant `MemoryStorage` would be N independent heaps in one process — a demo of the shape, not
 * the shape. Refusing is better than a version of this that only looks like it works.
 *
 * It is decided HERE, before `storage` exists, because shart (A) is about the ORDER: the shared
 * pool and listener are built FIRST and the default tenant is built ON them. Built the other way
 * round it kept a private pool of 10 and a `CommitListener` of its own — 11 connections per gateway
 * that no `rtdb_pg_pool_waiting` could see, on the tenant that is today's entire production.
 */
const multiTenant = process.env['RTDB_MULTI_TENANT'] === '1';
if (multiTenant && storageKind !== 'postgres') {
  throw new Error('RTDB_MULTI_TENANT=1 requires RTDB_STORAGE=postgres: a tenant is a schema.');
}

let tenantStorage: ((db: string) => Adapter) | undefined;
let storage: StorageAdapter;
/**
 * What SIGTERM has to close. In the single-tenant case the adapter owns its pool and its listener
 * and closing it is enough; sharing them moves that responsibility to whoever BUILT them, because
 * an adapter deliberately never ends a pool it borrowed.
 */
let closeStorage: () => Promise<unknown> = () => Promise.resolve(storage.close?.());
if (multiTenant) {
  const shared = await openSharedTenancy({ url: pgUrl(), limits, schema: defaultSchema, ...control });
  storage = shared.storage;
  tenantStorage = shared.tenantStorage;
  bindPoolWaiting(() => shared.pool.waitingCount);
  closeStorage = shared.close;
  process.stderr.write(
    `rtdb multi-tenant: ${shared.declared.length} declared databases, pool max ${shared.max}\n`,
  );
} else {
  storage = storageFromEnv();
}

/**
 * §8 fanout wiring. `RTDB_REDIS_URL` set -> this gateway joins the shard's bus; unset -> the
 * in-process path, unchanged (WORKLOAD §0.7). A URL we cannot reach is a boot failure, never a
 * silent island: `connect()` rejects here and the process exits non-zero.
 */
const redisUrl = process.env['RTDB_REDIS_URL'];
const redis = redisUrl ? await connectRedis(redisUrl) : undefined;

/**
 * §9 retention as a background chore. Off unless asked for: `RTDB_PRUNE_MS` names the interval, and
 * only Postgres has history to prune (memory prunes inline as it records). On the bus the gateway
 * runs it only while it holds leadership (WP4 Gate D ruling Q4); off the bus, always.
 */
const pruneMs = Number(process.env['RTDB_PRUNE_MS'] ?? 0);
const prune =
  pruneMs > 0 && storage instanceof PostgresStorage
    ? { intervalMs: pruneMs, run: (): Promise<unknown> => storage.prune() }
    : undefined;

// v1 runs shard 0; `RTDB_SHARD` exists so several independent shards can share one Redis, the same
// way `RTDB_PG_SCHEMA` lets them share one database.
const shard = process.env['RTDB_SHARD'] ?? 0;

// The other deploy-review knob: §8's leader lock TTL. Unset keeps `Leadership`'s 3000 ms.
const lockTtlMs = Number(process.env['RTDB_LOCK_TTL_MS'] ?? 0);

/**
 * §5.16: the gateway's default validator is `DevHs256Validator`, whose default secret is the
 * literal `'dev-secret'` (auth.ts) — a string that lives in the source and is therefore public
 * knowledge. That is the right default for the local battery and the harness, and a world-writable
 * database for anything else: whoever knows the secret can mint a token for any `sub`.
 *
 * The line drawn here is the SAME one `storageFromEnv` draws above. `RTDB_STORAGE=postgres` means
 * somebody is running this for real and a real deployment must carry its own secret, so that is a
 * boot failure. Memory storage means somebody is trying it out and loses everything on restart
 * anyway; that case is served better by a loud line than by a refusal to start.
 */
if (!process.env['RTDB_DEV_SECRET']) {
  if (storage instanceof PostgresStorage) {
    throw new Error(
      'RTDB_STORAGE=postgres requires RTDB_DEV_SECRET: without it the gateway verifies tokens with ' +
        "the public default secret 'dev-secret' and would accept a forged token for any user.",
    );
  }
  // stderr, NOT stdout: harness/scenario.ts:29 takes the child's FIRST STDOUT LINE as the port, so
  // a warning written there is read as the port and every chaos scenario that restarts a gateway
  // fails. Machine-readable stdout, diagnostics on stderr.
  process.stderr.write(
    "rtdb WARNING: RTDB_DEV_SECRET is unset, so tokens are verified with the public default secret " +
      "'dev-secret'. That is fine for local development. Never expose this gateway.\n",
  );
}

/**
 * Authorization. The gateway's default is `allowAll` (rules.ts) and `startGateway` takes it when
 * nothing is passed, so a deployment that configures no rules authenticates every client and then
 * authorizes all of them for everything: any token that can connect can write any path.
 *
 * That includes paths nobody declared. A namespace is not declared anywhere — it is the first
 * segment of a path and it exists as soon as something is written under it — so the server cannot
 * refuse an "unknown" namespace on its own. A rules module is where it becomes able to.
 *
 * Same line as the secret above, for the same reason: `RTDB_STORAGE=postgres` means a real
 * deployment, and a real deployment with no authorization is a world-writable database. Memory
 * storage is somebody trying it out, and a loud line serves them better than a refusal.
 *
 * The module must export `rules` (or a default) as a function. `rules/own-subtree.ts` is a working
 * example, and it is the smallest one that is not a toy.
 */
const rulesPath = process.env['RTDB_RULES'];
let rules: Rules | undefined;
if (rulesPath) {
  // Resolved against the WORKING DIRECTORY, not this module: `RTDB_RULES=rules/mine.ts` has to mean
  // what it looks like it means from the shell that typed it.
  const mod = (await import(pathToFileURL(resolve(rulesPath)).href)) as {
    rules?: unknown;
    default?: unknown;
  };
  const fn = mod.rules ?? mod.default;
  if (typeof fn !== 'function') {
    throw new Error(
      `RTDB_RULES=${rulesPath} loaded, but it exports no rules function — expected \`export const rules\` ` +
        'or a default export. Refusing to start rather than falling back to allowAll, which is what ' +
        'this variable exists to replace.',
    );
  }
  rules = fn as Rules;
} else if (storage instanceof PostgresStorage) {
  throw new Error(
    'RTDB_STORAGE=postgres requires RTDB_RULES: without it every authenticated client may read and ' +
      'write every path, including top-level namespaces nobody declared. See rules/own-subtree.ts.',
  );
} else {
  process.stderr.write(
    'rtdb WARNING: RTDB_RULES is unset, so every authenticated client can read and write the entire ' +
      'tree. Fine for local development. See rules/own-subtree.ts before exposing this.\n',
  );
}

/**
 * §5.20 Phase 1's tenancy switch. ON, a token that carries no `ns` claim may not read or write
 * anything (console subjects excepted — §5.9 governs those). OFF, this gateway is the
 * single-database deployment it has always been.
 *
 * It is NOT fail-closed the way `RTDB_RULES` above is, and that is deliberate rather than an
 * oversight: production `c0337c4` is serving app tokens that were minted before this claim existed,
 * so demanding it here would refuse every live client on the next deploy. The switch flips when the
 * console has re-minted those tokens with `ns` — which is Phase 1's other half, `/app-token`.
 */
const requireNs = process.env['RTDB_REQUIRE_NS'] === '1';

const gw = await startGateway({
  port,
  limits,
  storage,
  shard,
  requireNs,
  ...(gatewayCount > 1 ? { gatewayCount } : {}),
  ...(tenantStorage ? { tenantStorage } : {}),
  // Gate E's label for the default tenant: the schema this gateway was already serving.
  db: defaultSchema,
  ...(redis ? { redis } : {}),
  ...(prune ? { prune } : {}),
  ...(rules ? { rules } : {}),
  ...(lockTtlMs > 0 ? { lockTtlMs } : {}),
});

/**
 * WORKLOAD §2's `/metrics` + `/healthz`, on their own port: `/healthz` is the NLB target group's
 * check and `/metrics` is what the ops box scrapes — neither belongs on the public TLS listener.
 * Off by default so nothing that does not ask for it changes (the whole local battery included).
 */
const adminPort = Number(process.env['RTDB_ADMIN_PORT'] ?? 0);
const admin =
  adminPort > 0 ? await startAdminServer({ port: adminPort, storage, db: defaultSchema }) : null;

// The runner reads this line to learn the port when it asked for an ephemeral one.
process.stdout.write(`rtdb listening ${gw.port}\n`);
if (admin) process.stdout.write(`rtdb admin ${adminPort}\n`);

for (const signal of ['SIGTERM', 'SIGINT'] as const) {
  process.on(signal, () => {
    gw.close();
    admin?.close();
    // The pool and the LISTEN connection outlive the socket; a SIGTERMed gateway must not leave
    // backends open behind it. SIGKILL gets no such courtesy, which is the point of the chaos suite.
    void Promise.resolve(closeStorage())
      .then(() => redis?.close())
      .catch(() => undefined)
      .finally(() => process.exit(0));
  });
}
