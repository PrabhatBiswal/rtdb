import { randomInt } from 'node:crypto';
import { readFileSync } from 'node:fs';
import pg from 'pg';
import { DEFAULT_LIMITS, type Limits } from '../protocol/limits.ts';
import {
  ancestorsInclusive,
  isRelevant,
  joinPath,
  MAX_DATABASE_NAME,
  SCHEMA_NAME,
  validateDatabaseName,
} from '../protocol/path.ts';
import type { Json } from '../protocol/frames.ts';
import type {
  AckResult,
  CasResult,
  CasWrite,
  GroupWrite,
  OplogEntry,
  SnapshotRead,
  StorageAdapter,
  WriteOp,
} from './adapter.ts';
import { flatten, type Leaf, unflatten } from './tree.ts';

const SCHEMA_SQL = readFileSync(new URL('./schema.sql', import.meta.url), 'utf8');

/**
 * §5.22 Gate A: the shard's CONTROL schema — the one schema that belongs to no tenant.
 *
 * `rtdb_control` and not `public`, because production's tenant schema IS `public` today and a
 * control schema that lands there would make this gate a no-op exactly where it matters. Not `rtdb`
 * either: once Gate D derives a tenant's schema from its client-visible database name, a plausible
 * name has to be unable to collide with this one, and the `rtdb_` prefix plus a word no product
 * surface uses is what buys that. The constructor refuses a tenant schema equal to it, so the
 * collision is closed by construction rather than by hoping the derivation stays prefixed.
 */
export const DEFAULT_CONTROL_SCHEMA = 'rtdb_control';

/**
 * §5.22 Gate B: the search_path a pooled connection carries when NO transaction has claimed it.
 *
 * It names a schema that is never created, and that is the entire footgun defence. A shared pool
 * means a connection outlives the tenant that last borrowed it, so a transaction that forgot to set
 * its own search_path would write into whoever held the connection before it — silently, and
 * across tenants. Measured, both arms: with the pool defaulting to a real tenant's schema the
 * forgotten `SET` returned "ok" and the row landed in the previous tenant's table; with the pool
 * defaulting HERE it fails at the first unqualified relation with `relation "…" does not exist`.
 *
 * This is why the fix is not an assert. An assert has the same failure mode as the thing it guards
 * — it must be remembered in every transaction. This has to be remembered once, in one place, and
 * it turns the leak into an error inside the offending transaction.
 */
export const NO_TENANT_SCHEMA = 'rtdb_no_tenant';

/**
 * §5.22 Gate B: the ONE place a pool for this adapter is built, and it is exported so that the pool
 * an adapter makes for itself and the pool Gate D shares between N tenants are the same object,
 * built the same way. Two constructions would be two behaviours, and only one of them would be the
 * one under test.
 */
export const createPool = (url: string, max = 10): pg.Pool =>
  new pg.Pool({
    connectionString: url,
    max,
    /**
     * §5.11 Gate B. Unset, pg-pool skips its timeout branch entirely (`pg-pool/index.js:206`) and
     * a waiter queues FOREVER — so an exhausted pool did not slow the gateway down, it hung it,
     * with no error for any layer above to act on. With it, the wait rejects at `:225` and the
     * containment built in Gate A takes over: a write retries then abandons with a log line, a
     * listen repairs via §3 resync, and the connection stays up.
     *
     * 500ms, and all three bounds are measured:
     *  - FLOOR (measured): pool acquisition p99 is 0.143ms at 500 concurrent listens locally,
     *    worst single acquisition 8.3ms. 500ms is ~3,500x that p99 — healthy traffic cannot reach
     *    it, even allowing an order of magnitude for RDS holding connections longer than a local
     *    server does.
     *  - CEILING: healthz's own probe budget is 2000ms (`metrics.ts`). Going above it would make
     *    this dead weight — a wait that long fails the health check and the NLB pulls the gateway
     *    before the timeout could matter.
     *  - PRODUCT (§5.11 Gate A): the write path retries 3 times inside the serial commit chain, so
     *    the head-of-line stall is ~3 x (timeout + spacing) — about 1.6s here, against ~6.1s if
     *    this were 2000ms. The user approved Gate B having seen that arithmetic.
     */
    connectionTimeoutMillis: 500,
    /**
     * §5.22 Gate B: NOT this tenant's schema, which is what it used to be (`-c search_path=<schema>`)
     * and which was the whole reason the budget was `10 x N` — a connection was bound to a tenant
     * before it was ever borrowed. It now names nothing, and every transaction claims the
     * connection for itself with `SET LOCAL`. See `NO_TENANT_SCHEMA`.
     */
    options: `-c search_path=${NO_TENANT_SCHEMA}`,
      });

/**
 * §5.22 Gate B / §5.21's `P >= N + reads + 1`, as code rather than as a number somebody remembers.
 *
 * The two terms are different KINDS and sizing them alike is the mistake this exists to prevent:
 *
 *  - `tenants` is a STRUCTURAL FLOOR. Each tenant's `WritePipeline` serializes onto its own chain
 *    (`write.ts` `#chain`), and a chain holds one connection for the whole of its transaction. Below
 *    N, tenants demonstrably queue on each other — which is the cross-tenant coupling Phase 3 exists
 *    to remove, moved from the `rev_counter` row to the pool, and invisible because a pool wait
 *    looks like a slow commit rather than a refusal.
 *  - `reads` is QUEUEING HEADROOM. The pool-level reads are single statements with no transaction,
 *    so they need enough connections for concurrent demand, not one per tenant.
 *
 * The `+ 1` is the listen client's slot, kept so a shard sized to the floor still has one to spare.
 */
export const sharedPoolSize = (tenants: number, reads = 4): number => tenants + reads + 1;

/**
 * One `remove` handler per POOL, however many adapters share it.
 *
 * Gate B let N tenants share a pool, and each of them was doing `pool.on('remove', …)` in its own
 * constructor. Two defects, both measured on 14 tenants over one pool: Node warns at the eleventh
 * (`MaxListenersExceededWarning`, because an EventEmitter is being used as a fan-out registry), and
 * `close()` never detached, so under Gate D a tenant that comes and goes leaves its handler on a
 * pool that outlives it — a real leak, not just a noisy one.
 *
 * The handler itself is still needed: Postgres reuses backend PIDs, so a dead connection's PID must
 * be forgotten by every adapter or a stale one silently swallows a real notification from another
 * gateway. What changes is that the pool carries ONE handler that fans out to a set, and each
 * adapter holds only its own membership — which it can then give back.
 */
const pidForgetters = new WeakMap<pg.Pool, Set<(pid: number) => void>>();

/**
 * Built HERE, in a function whose only local is the set, and the placement IS the fix.
 *
 * V8 gives every closure created in one invocation a SHARED context holding that invocation's
 * variables. Written inline inside `forgetPidsOnRemove`, the pool's long-lived `remove` handler and
 * the short-lived unhook it returns share one context — so the handler, which lives as long as the
 * pool, transitively pins the FIRST adapter's `forget` and through it that adapter, for the pool's
 * whole life. That adapter's own `close()` cannot undo it: the reference is not in the set, it is in
 * the context the handler carries, and nothing in the set's API can reach it.
 *
 * Measured, `WeakRef` + `gc()` over 14 adapters on one pool: inline, 1 of 14 survives and it is
 * ALWAYS index 0 — at n=1, n=3 and n=14, and whichever order they are closed in. Hoisted here, 0 of
 * 14. One adapter rather than N, so it is bounded — and still a promise `close()` was not keeping.
 * Under Gate D it means the first tenant to touch a gateway's pool is never collected.
 */
const fanOutRemove =
  (subscribers: Set<(pid: number) => void>) =>
  (c: pg.PoolClient): void => {
    const pid = (c as TrackedClient)[BACKEND_PID];
    if (pid !== undefined) for (const forget of subscribers) forget(pid);
  };

function forgetPidsOnRemove(pool: pg.Pool, forget: (pid: number) => void): () => void {
  let set = pidForgetters.get(pool);
  if (!set) {
    set = new Set<(pid: number) => void>();
    pidForgetters.set(pool, set);
    pool.on('remove', fanOutRemove(set));
  }
  const subscribers = set;
  subscribers.add(forget);
  return () => void subscribers.delete(forget);
}

/**
 * The registry, and it is the only DDL in this file rather than in `schema.sql` for one reason:
 * `schema.sql` is applied through the connection's `search_path` and is therefore a TENANT's
 * schema, statement for statement. This table is not a tenant's — every tenant on the shard reads
 * it and the console writes it — so it is qualified explicitly and does not care what the
 * search_path says. Keeping it in the file would have meant interpolating a schema name into the
 * file, which stops it being SQL you can run.
 *
 * §5.19's reasoning is unchanged and still the point: a database used to be a side-effect of data
 * (whatever path was written first), which meant a client could not hand an empty one to a team, a
 * team could delete itself out of existence, and a usage panel had no row to draw for a database
 * holding nothing. `topNodes` unions this with the derived list, so declaring stays additive.
 */
const CONTROL_SQL = (control: string): string => `
CREATE TABLE IF NOT EXISTS ${control}.databases (
  name       TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_by TEXT        NOT NULL
)`;

/**
 * §5.24 Gate C: the per-database quota override, added SEPARATELY from the table above.
 *
 * `ADD COLUMN IF NOT EXISTS` and not a new column in `CONTROL_SQL`, because `CREATE TABLE IF NOT
 * EXISTS` does nothing at all to a table that already exists — a shard that has been declaring
 * databases since §5.19 would never grow the column, and the override would silently be null
 * forever on precisely the deployments that have data. Idempotent, so every boot is safe.
 *
 * NULL means "the shard default" (§5.23's `QUOTA_ACQ_PER_SEC`), which is a different statement from
 * any number: it says nobody has decided for this database, so it follows the shard when the shard
 * changes. A column defaulted to 64 would freeze today's number into every row.
 */
const CONTROL_QUOTA_SQL = (control: string): string =>
  `ALTER TABLE ${control}.databases ADD COLUMN IF NOT EXISTS quota_acq_per_sec INTEGER`;

/** `oplog.op` is a SMALLINT (§8); these two values are the whole mapping. */
export const OP_CODE = { put: 0, merge: 1 } as const;

const OP_NAME = ['put', 'merge'] as const satisfies readonly WriteOp[];

/**
 * §5.22 Gate B: a function of the tenant, because both call sites are POOL-LEVEL reads — outside
 * any transaction, so outside the one place `SET LOCAL search_path` runs. The statements inside
 * `#tx` stay unqualified on purpose: the preamble has already claimed that connection.
 */
const oplogColumns = (tenant: string): string =>
  `SELECT rev, path, op, value, write_id, ts FROM ${tenant}.oplog`;

interface OplogRowDb {
  rev: string;
  path: string;
  op: number;
  value: Json;
  write_id: string;
  ts: Date;
}

const toEntry = (r: OplogRowDb): OplogEntry => ({
  rev: Number(r.rev),
  path: r.path,
  op: OP_NAME[r.op] as WriteOp,
  value: r.value,
  writeId: r.write_id,
  ts: r.ts.getTime(),
});

/** Caches a connection's backend PID on the client object itself — it lives exactly as long. */
const BACKEND_PID = Symbol('rtdb.backendPid');
type TrackedClient = pg.PoolClient & { [BACKEND_PID]?: number };

/** An oplog row on its way into the insert — a committed `GroupWrite` with its rev. */
interface OplogRow {
  rev: number;
  path: string;
  op: WriteOp;
  value: Json;
  writeId: string;
}

/**
 * The LIKE pattern matching every strict descendant of `path`.
 *  - `path` is user data and `%`, `_` and `\` are all legal path characters (`MPK_1010` is a real
 *    path here), so the prefix MUST be escaped before it becomes a pattern.
 *  - root ("") is an ancestor of EVERYTHING, which `'/%'` would say the opposite of. Handling it
 *    here is what keeps every call site a single branch-free predicate.
 */
export const likeDescendants = (path: string): string =>
  path === '' ? '%' : `${path.replace(/[\\%_]/g, '\\$&')}/%`;

/** One subtree replacement resolved out of a write: what lands at `path`, under `rev`. */
interface Target {
  path: string;
  leaves: Leaf[];
  rev: number;
}

/**
 * May these targets be applied as one batch? Only if none is at-or-under another — see
 * `#applyTargets` for why that is the exact condition. `isRelevant` is §3's predicate and already
 * means "one is at-or-under the other"; it is true for a path against itself, so a group that
 * writes the same path twice correctly falls back to ordered application.
 *
 * ponytail: O(n^2), and n is a commit group (~5) or a merge's key count. A sort-and-scan is the
 * upgrade if either ever gets large enough to matter.
 */
const prefixDisjoint = (paths: string[]): boolean => {
  for (let i = 0; i < paths.length; i++) {
    for (let j = i + 1; j < paths.length; j++) {
      if (isRelevant(paths[i] as string, paths[j] as string)) return false;
    }
  }
  return true;
};

/** "`col` is at or under the path in $1" — reading a snapshot, and keeping `nodes` prefix-free. */
const atOrUnder = (col: string): string => `(${col} = $1 OR ${col} LIKE $2 ESCAPE '\\')`;

/**
 * §3 relevance over the oplog: an entry at/under the path ($3, the escaped descendant pattern), or
 * an ancestor of it ($2, the <=33 expanded ancestor paths). §8 gives one index for each half, and
 * `plans.pgtest.ts` EXPLAINs this exact string to keep it that way.
 */
/**
 * The nodes half of a write, as three shipped strings — exported for the same reason
 * `RELEVANT_SQL` is: `plans.pgtest.ts` EXPLAINs THESE, not a copy that can drift from them.
 *
 * `$1` is every path to remove exactly (the targets and their ancestors); the descendant half is a
 * RANGE over `text_pattern_ops`, never a LIKE. See `#deleteTargets` for why that distinction is the
 * whole point, and `TOPNODES_SQL` for the byte-order argument the bounds rest on.
 */
export const DELETE_SOLO_SQL = `DELETE FROM nodes WHERE path = ANY($1::text[]) OR (path ~>=~ $2 AND path ~<~ $3)`;

export const DELETE_EXACT_SQL = `DELETE FROM nodes WHERE path = ANY($1::text[])`;

export const DELETE_RANGE_SQL = `DELETE FROM nodes n
   USING unnest($1::text[], $2::text[]) AS t(lo, hi)
   WHERE n.path ~>=~ t.lo AND n.path ~<~ t.hi`;

export const RELEVANT_SQL = `(path = ANY($2::text[]) OR path LIKE $3 ESCAPE '\\')`;

/**
 * DISTINCT top-level segments, as a SKIP SCAN — never a DISTINCT over the leaves.
 *
 * `SELECT DISTINCT split_part(path,'/',1) FROM nodes` is a sequential scan of every row in the
 * shard for an answer that is a handful of strings: 21.6ms over 200k rows and growing forever. This
 * walks the path index instead — one lookup per NAMESPACE, so the cost follows the namespace count
 * rather than the leaf count. Measured on the production shard: 0.226ms, `Index Only Scan using
 * nodes_path_pattern`, five loops for five namespaces.
 *
 * ## Why the pattern operators, and not `>=`
 *
 * The walk depends on one fact: every path in namespace `ns` (`ns` itself and `ns/…`) sorts BELOW
 * `ns || '0'`, because `/` is 0x2F and `0` is 0x30. That is true of BYTE order. It is not true of
 * every collation — glibc's `en_US.UTF-8` ignores punctuation at the primary level, so there
 * `'MPK_1010/a' >= 'MPK_10100'` is TRUE, the walk lands back inside the namespace it just left, and
 * the recursion never terminates.
 *
 * This was not theoretical. Written with plain `>=`, every local test passed — macOS's
 * `en_US.UTF-8` happens to behave byte-wise — and the first call on RDS, whose collation carries the
 * SAME NAME and different semantics, span for four and a half minutes until it was cancelled by hand.
 * Two servers, one collation name, opposite answers.
 *
 * `~>=~` and `ORDER BY … USING ~<~` are the `text_pattern_ops` family: byte semantics by definition,
 * identical on every platform, and served by the `nodes_path_pattern` index that already exists for
 * exactly this reason. `>=` and not `>` still matters within that: a namespace literally called
 * `ns0` sits exactly ON the boundary and a strict `>` would skip it.
 *
 * The depth bound is insurance, not decoration. If some future data or operator combination ever
 * re-creates a cycle, this returns a truncated list instead of pinning a core forever — a bad answer
 * beats no answer, and a hung backend on the production shard is what it cost to learn that.
 */
export const topNodesSql = (control: string, tenant: string): string => `
WITH RECURSIVE ns AS (
  SELECT 1 AS depth,
         (SELECT split_part(path, '/', 1) FROM ${tenant}.nodes ORDER BY path USING ~<~ LIMIT 1) AS name
  UNION ALL
  SELECT ns.depth + 1,
         (SELECT split_part(n.path, '/', 1) FROM ${tenant}.nodes n
           WHERE n.path ~>=~ (ns.name || '0') ORDER BY n.path USING ~<~ LIMIT 1)
    FROM ns WHERE ns.name IS NOT NULL AND ns.depth < 5000
)
SELECT name FROM ns WHERE name IS NOT NULL
UNION
SELECT name FROM ${control}.databases
ORDER BY name`;

/** Leaves of a subtree as `{path, value}`, ready for `unflatten`. One consistent read per call site. */
const LEAVES = (col: string): string =>
  `coalesce(json_agg(json_build_object('path', ${col}.path, 'value', ${col}.value)), '[]')`;

export interface PostgresOptions {
  /** libpq connection string; the database must already exist. */
  url: string;
  limits?: Limits;
  /**
   * Postgres schema holding the tables. Production uses `public`; tests give every adapter its own
   * so one database can host many independent stores (WP4 Gate A isolation).
   */
  schema?: string;
  /**
   * §5.22 Gate A: the schema holding the `databases` registry — ONE per shard, shared by every
   * tenant on it. Defaults to `DEFAULT_CONTROL_SCHEMA`, which is what makes Gate D right by
   * default: N adapters constructed without thinking about it still share one registry, rather
   * than each growing a private copy that nothing would notice.
   */
  controlSchema?: string;
  /**
   * §5.22 Gate B: a pool to BORROW rather than create. Given one, this adapter never ends it — the
   * owner does — which is what lets N tenants share the connections that used to be `10 x N`.
   * Its default `search_path` must be `NO_TENANT_SCHEMA`; `sharedPoolSize` sizes it.
   *
   * Unset, the adapter makes its own, exactly as it always has. That is what keeps the 38 test
   * files and today's single-tenant deployment unchanged.
   */
  pool?: pg.Pool;
  /**
   * §5.22 Gate C: a `LISTEN` connection to SHARE rather than open. One client carries every
   * tenant's channel, so N tenants cost one connection here instead of N. Given one, this adapter
   * never closes it — the owner does; it only stops listening to its own channel.
   */
  listener?: CommitListener;
  poolMax?: number;
}


const COMMIT_CHANNEL_PREFIX = 'rtdb_commit_';

/**
 * Postgres truncates an identifier at 63 BYTES, and it does it with a NOTICE rather than an error —
 * so two channels that differ only past byte 63 are one channel, and nothing says so.
 *
 * That is not a curiosity here, it is this gate's own defect coming back: two tenant schemas sharing
 * their first 51 characters would `LISTEN`/`NOTIFY` on the same truncated name and cross-wake each
 * other exactly as the unqualified `rtdb_commit` did. Measured — `LISTEN <64 chars ending x>` then
 * `NOTIFY <same 63 chars ending y>` delivers, and the notification names the 63-byte form.
 *
 * The bound therefore belongs on the SCHEMA NAME, in the constructor's existing guard, derived from
 * this prefix rather than written as 51. Two rules that must agree and are written separately are
 * two rules that will disagree.
 */
/**
 * §5.26: re-exported from `path.ts`, where the database-name rule now lives, so the two cannot
 * drift. They already had: a name this file refused was one `validateDatabaseName` accepted, and
 * the disagreement surfaced as a 1011 at hello with an undeletable registry row behind it.
 */
export const MAX_SCHEMA_NAME = MAX_DATABASE_NAME;

/** §5.22 Gate C: the channel a tenant's commits are announced on. One per tenant schema. */
export const commitChannel = (tenant: string): string => `${COMMIT_CHANNEL_PREFIX}${tenant}`;

/**
 * §5.22 Gate C: ONE `LISTEN` connection carrying every tenant's channel.
 *
 * The channel used to be the literal `rtdb_commit`, and LISTEN/NOTIFY in Postgres is scoped to the
 * DATABASE, not the schema — so with one schema per tenant every commit woke every OTHER tenant's
 * dispatcher, each wake costing one `readOplogSince` that returns nothing. Measured before this
 * gate: 5 commits in schema A produced 5 wake-ups in schema B while B's oplog was empty. O(N)
 * amplification on the hot write path of a shard already at 96% of its lock ceiling.
 *
 * Qualifying the channel kills that, and it kills the OTHER half at the same time: one client can
 * hold N channels, so N tenants no longer need N dedicated connections. That single edit is the
 * reason `(f)` came out SCHEMA — it was the one place the database branch was winning.
 *
 * RECONNECTION IS PART OF THIS GATE, and it is the note that used to sit on `#listen` saying a
 * dropped connection was not re-established. That was affordable when a dropped connection blinded
 * ONE dispatcher in a deployment where the local listeners carried every wake-up anyway. One client
 * carrying N tenants blinds N of them, and past Gate D there is a second gateway whose commits only
 * ever arrive this way.
 *
 * On reconnect every channel is poked once, unconditionally: notifications raised during the gap are
 * gone, and a poke is cheap (one oplog read that usually returns nothing) where a missed commit is
 * a subscription that never converges.
 */
export class CommitListener {
  #client: pg.Client | null = null;
  #channels = new Map<string, Set<(pid: number) => void>>();
  #starting: Promise<void> | null = null;
  #retry: NodeJS.Timeout | null = null;
  #stopped = false;

  constructor(
    private readonly url: string,
    /** Reconnect spacing. Fixed, not backed off: this is a poke channel, not a load-bearing query. */
    private readonly retryMs = 500,
    /**
     * `application_name`, so this connection says what it is in `pg_stat_activity`. It is one
     * long-lived connection per gateway that runs no queries after its LISTENs, which is exactly
     * what an idle-connection hunt kills first — and the operator deciding that has nothing else to
     * go on. Tests use it to terminate THIS listener's backend and no other.
     */
    private readonly appName = 'rtdb-commit-listener',
  ) {}

  /** Subscribe to one tenant's channel. Returns the unsubscribe. */
  async listen(channel: string, onCommit: (pid: number) => void): Promise<() => void> {
    let subs = this.#channels.get(channel);
    if (!subs) {
      subs = new Set();
      this.#channels.set(channel, subs);
    }
    subs.add(onCommit);
    await this.#ensure();
    // A client that is already up has not heard of this channel yet; one that is coming up will
    // pick it up in `#attach`. Both paths, because `#ensure` resolves either way.
    await this.#client?.query(`LISTEN ${channel}`).catch(() => undefined);
    return () => {
      subs?.delete(onCommit);
      if (subs?.size === 0) {
        this.#channels.delete(channel);
        void this.#client?.query(`UNLISTEN ${channel}`).catch(() => undefined);
      }
    };
  }

  async close(): Promise<void> {
    this.#stopped = true;
    if (this.#retry) clearTimeout(this.#retry);
    this.#retry = null;
    this.#channels.clear();
    const c = this.#client;
    this.#client = null;
    await c?.end().catch(() => undefined);
    // And whatever was mid-connect: `#attach` checks `#stopped` after its own await, so joining
    // here is what makes `close()` mean "no connection of mine is open when this resolves".
    await this.#starting?.catch(() => undefined);
  }

  #ensure(): Promise<void> {
    if (this.#stopped || this.#client) return Promise.resolve();
    return (this.#starting ??= this.#attach().finally(() => {
      this.#starting = null;
    }));
  }

  async #attach(): Promise<void> {
    if (this.#stopped) return;
    const c = new pg.Client({ connectionString: this.url, application_name: this.appName });
    // A dead poke connection must never take the process with it — and `error` is also how a
    // connection that dies while idle announces itself, which is what arms the reconnect.
    c.on('error', () => this.#reattach(c));
    c.on('end', () => this.#reattach(c));
    c.on('notification', (msg) => {
      for (const cb of this.#channels.get(msg.channel) ?? []) cb(msg.processId);
    });
    try {
      await c.connect();
      // `close()` can land INSIDE that await, and when it does it sees `#client` still null and
      // ends nothing — so this connection would survive its own listener and hold the process open.
      // A test file whose every subtest passed and whose FILE then timed out is what that looks
      // like from the outside; there is no error anywhere.
      if (this.#stopped) return void (await c.end().catch(() => undefined));
      this.#client = c;
      for (const channel of this.#channels.keys()) await c.query(`LISTEN ${channel}`);
    } catch {
      this.#client = null;
      await c.end().catch(() => undefined); // a half-open client is still a handle
      this.#schedule();
    }
  }

  #reattach(dead: pg.Client): void {
    if (this.#client !== dead) return; // an old client's death event, already replaced
    this.#client = null;
    this.#schedule();
  }

  #schedule(): void {
    if (this.#stopped || this.#retry) return;
    this.#retry = setTimeout(() => {
      this.#retry = null;
      void this.#ensure().then(() => {
        // The gap swallowed every notification raised inside it. Poke everyone once rather than
        // reason about what was missed: the dispatcher re-reads the oplog, so a spurious poke costs
        // one query and a missed one costs a subscription that never converges.
        if (this.#client) for (const subs of this.#channels.values()) for (const cb of subs) cb(-1);
      });
    }, this.retryMs);
    this.#retry.unref();
  }
}

/**
 * §8's schema as a StorageAdapter. Every method is ONE transaction — where a method is a single
 * statement, that statement IS the transaction (and its single MVCC snapshot is what §3's snapshot
 * atomicity rule asks for).
 *
 * Where memory.ts got atomicity for free from Node's event loop, here it is bought explicitly:
 * BEGIN/COMMIT, the `rev_counter` row lock, and the `write_id` unique index.
 */
export class PostgresStorage implements StorageAdapter {
  readonly #pool: pg.Pool;
  /** True only for a pool this adapter made. A borrowed one outlives us and must not be ended. */
  readonly #ownsPool: boolean;
  readonly #schema: string;
  readonly #control: string;
  /** Gate B's commitGroup flattens with these (§9). */
  readonly #limits: Limits;
  /** Resolves to the epoch; also the "schema is applied" latch. Every public method awaits it. */
  #ready: Promise<number> | null = null;
  readonly #listeners = new Set<() => void>();

  /**
   * §5.17: the ordered fallback used to be silent. A workload that collides on every commit —
   * presence, a cursor, a counter, anything that writes ONE path repeatedly — would pay the old
   * per-write cost forever with nothing anywhere to say so. `groups` is the denominator: commits
   * whose nodes work had more than one target, i.e. the ones that COULD have batched.
   */
  readonly #applyStats = { groups: 0, orderedFallbacks: 0 };
  readonly #url: string;
  /**
   * The backend PIDs of our OWN pool connections. Our commits already woke the local listeners
   * synchronously, so their NOTIFY must not wake them a second time — §8's poke is for the gateways
   * that did NOT do the write. Filtering on the sender's PID keeps the payload contentless, which
   * is the ruling (WORKLOAD §4).
   */
  readonly #ownPids = new Set<number>();
  readonly #listener: CommitListener;
  /** True only for a listener this adapter made. A borrowed one outlives us. */
  readonly #ownsListener: boolean;
  /** Stops this adapter's channel subscription. Null until `onCommit` first wires it. */
  #unlisten: (() => void) | null = null;
  /** Set by `close()`. A subscription that resolves after it must not be kept. */
  #closed = false;
  /** Detaches this adapter from the pool's shared `remove` fan-out. */
  readonly #unhookPool: () => void;

  constructor(opts: PostgresOptions) {
    this.#url = opts.url;
    this.#schema = opts.schema ?? 'public';
    this.#control = opts.controlSchema ?? DEFAULT_CONTROL_SCHEMA;
    // The schema names are interpolated into DDL; nothing but an identifier may reach those
    // strings. This same guard is what makes the schema-qualified `databases` reference in
    // `topNodesSql` safe — one check, not a second one beside it.
    // §5.22 Gate C added the LENGTH half to this same guard rather than beside it: the schema name
    // becomes a NOTIFY channel, Postgres truncates identifiers at 63 bytes silently, and two schemas
    // sharing a long enough prefix would land on one channel and cross-wake — the very defect this
    // phase removed. `MAX_SCHEMA_NAME` derives from the channel prefix; nobody writes 51.
    for (const [what, name] of [['schema', this.#schema], ['control schema', this.#control]] as const) {
      if (!SCHEMA_NAME.test(name) || name.length > MAX_SCHEMA_NAME) {
        throw new Error(`illegal ${what} name: ${name}`);
      }
    }
    // A tenant schema that IS the control schema puts the list of all tenants back inside one of
    // them — the exact shape Gate A exists to undo — and it would do it silently.
    if (this.#schema === this.#control) {
      throw new Error(`schema ${this.#schema} is the control schema; a tenant may not own it`);
    }
    this.#limits = opts.limits ?? DEFAULT_LIMITS;
    this.#ownsPool = opts.pool === undefined;
    this.#ownsListener = opts.listener === undefined;
    this.#listener = opts.listener ?? new CommitListener(opts.url);
    this.#pool = opts.pool ?? createPool(opts.url, opts.poolMax ?? 10);
    // A connection that goes away takes its PID with it: Postgres reuses PIDs, and a stale one here
    // would silently swallow a real notification from another gateway. Registered through the pool's
    // ONE handler, and given back in `close()` — see `forgetPidsOnRemove`.
    this.#unhookPool = forgetPidsOnRemove(this.#pool, (pid) => this.#ownPids.delete(pid));
  }

  head(): Promise<number> {
    return this.#counter('v');
  }

  /** §2: the generation is read once at startup and never moves while the store lives. */
  epoch(): Promise<number> {
    return this.#init();
  }

  /** Cumulative since process start; see `#applyStats`. Read by the gateway's `/metrics`. */
  get applyStats(): { groups: number; orderedFallbacks: number } {
    return { ...this.#applyStats };
  }

  /**
   * §5.19: declare a database. Idempotent ON PURPOSE — re-declaring one a client already owns is
   * not an error, it is a no-op, and making it an error would only teach the console to guess.
   */
  async declareDatabase(name: string, by: string, quotaAcqPerSec?: number | null): Promise<void> {
    // The registry is the door a name comes through, so the rule lives on the door and not only on
    // the admin route in front of it (§5.22 Gate E). A reserved `_` name reaching the table would
    // put a real database's numbers in a synthetic metrics label, silently.
    const bad = validateDatabaseName(name, this.#limits);
    if (bad) throw new Error(`illegal database name: ${bad}`);
    await this.#init();
    /**
     * §5.24 Gate C. Re-declaring stays a NO-OP on the name (§5.19) and now also on the quota: an
     * `ON CONFLICT DO UPDATE` here would let a second `POST /databases` with no quota field silently
     * reset an override somebody set on purpose. Changing a quota is a different act from declaring
     * a database, and it does not have a door yet — which is the honest state, not a hidden one.
     */
    await this.#pool.query(
      `INSERT INTO ${this.#control}.databases (name, created_by, quota_acq_per_sec)
       VALUES ($1, $2, $3) ON CONFLICT (name) DO NOTHING`,
      [name, by, quotaAcqPerSec ?? null],
    );
  }

  /**
   * §5.24 Gate C: one database's registry row. `null` quota means "the shard's default" — a
   * different statement from any number, because it follows the shard when the shard changes.
   *
   * A name that is not in the registry answers `null` rather than throwing: the DEFAULT tenant is
   * exactly that case (it is the schema this gateway was configured with, never declared), and it
   * runs on the shard default like any undecided database.
   */
  async describeDatabase(name: string): Promise<{ quotaAcqPerSec: number | null } | null> {
    await this.#init();
    const { rows } = await this.#pool.query<{ quota_acq_per_sec: number | null }>(
      `SELECT quota_acq_per_sec FROM ${this.#control}.databases WHERE name = $1`,
      [name],
    );
    const row = rows[0];
    return row ? { quotaAcqPerSec: row.quota_acq_per_sec === null ? null : Number(row.quota_acq_per_sec) } : null;
  }

  /**
   * §5.24: live bytes per database, for every tenant on this shard, in ONE catalogue query.
   *
   * `pg_total_relation_size` on the `nodes` relation and nothing else: the oplog is §9 retention
   * that shrinks on its own, so billing it would bill a client for our durability window, and
   * `rev_counter` is a row. The join is `pg_class` to `pg_namespace` because a tenant IS a schema
   * (§5.22) — so "every database on this shard" is a `GROUP BY nspname` rather than N round trips
   * on a scrape path. Catalogue-only: it reads sizes, never a tenant's rows.
   *
   * `pg_total_relation_size` INCLUDES the indexes and TOAST, which is the honest number for
   * "what this database costs on disk" and larger than the JSON a client would say they stored.
   * The RUNBOOK says so where an operator comparing it against `\dt+` would otherwise file a bug.
   */
  async storageBytes(): Promise<Record<string, number>> {
    await this.#init();
    const { rows } = await this.#pool.query<{ db: string; bytes: string }>(
      `SELECT n.nspname AS db, sum(pg_total_relation_size(c.oid))::text AS bytes
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'nodes' AND c.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        GROUP BY n.nspname`,
    );
    return Object.fromEntries(rows.map((r) => [r.db, Number(r.bytes)]));
  }

  /**
   * §5.22 Gate D: the registry alone, unqualified by what has data. `topNodes` UNIONs this with the
   * derived names; whoever is sizing something wants only this half.
   */
  async listDeclared(): Promise<string[]> {
    await this.#init();
    const { rows } = await this.#pool.query<{ name: string }>(
      `SELECT name FROM ${this.#control}.databases ORDER BY name`,
    );
    return rows.map((r) => r.name);
  }

  /** §5.6's sidebar. Skip scan over the path index — see `topNodesSql` for why, and why not counts. */
  async topNodes(): Promise<string[]> {
    // `#init` first, which this did not do before Gate A and now must: the statement reads the
    // CONTROL schema's `databases`, and a console asking for the sidebar before any adapter has
    // applied its schema is the ordinary first request on a fresh shard, not an edge case.
    await this.#init();
    const { rows } = await this.#pool.query<{ name: string }>(topNodesSql(this.#control, this.#schema));
    return rows.map((r) => r.name);
  }

  prunedThroughRev(): Promise<number> {
    return this.#counter('pruned_through');
  }

  async readSnapshot(path: string): Promise<SnapshotRead> {
    await this.#init();
    // ONE statement, so `rev` and the leaves come from ONE MVCC snapshot (§3): because a commit
    // writes nodes and the counter in the same txn, seeing rev N means seeing all of N's effects.
    const { rows } = await this.#pool.query<{ rev: string; leaves: Leaf[] }>(
      `SELECT (SELECT v FROM ${this.#schema}.rev_counter WHERE shard = 0) AS rev,
              (SELECT ${LEAVES('n')} FROM ${this.#schema}.nodes n WHERE ${atOrUnder('n.path')}) AS leaves`,
      [path, likeDescendants(path)],
    );
    const row = rows[0] as { rev: string; leaves: Leaf[] };
    return { value: unflatten(path, row.leaves), rev: Number(row.rev) };
  }

  /**
   * §4 step 2, in ONE transaction: dedup resolves FIRST so the counter is taken for exactly the
   * number of NEW writes (gap-free, §1), then revs are handed out in arrival order and the oplog and
   * `nodes` are written together.
   */
  async commitGroup(writes: GroupWrite[]): Promise<AckResult[]> {
    await this.#init();
    const results = await this.#tx(async (c) => {
      // The counter row is locked BEFORE the dedup lookup, not just before the take. Two connections
      // replaying one writeId would otherwise both read "new", both take a rev, and the loser would
      // die on the unique index having already burned a rev nothing can fill — a permanent gap.
      await this.#lockCounter(c);

      const known = await this.#priorRevs(c, writes.map((w) => w.writeId));
      const inBatch = new Map<string, number>();
      const isNew: boolean[] = [];
      let fresh = 0;
      for (const w of writes) {
        const id = w.writeId.toLowerCase();
        const dup = known.has(id) || inBatch.has(id);
        isNew.push(!dup);
        if (!dup) {
          inBatch.set(id, -1); // a twin later in this same batch resolves to the rev set below
          fresh++;
        }
      }

      // §4 step 2: `v = v + N RETURNING v`, once, for the whole batch.
      let next = 0;
      if (fresh > 0) {
        const { rows } = await c.query<{ v: string }>(
          'UPDATE rev_counter SET v = v + $1 WHERE shard = 0 RETURNING v',
          [fresh],
        );
        next = Number((rows[0] as { v: string }).v) - fresh + 1;
      }

      const entries: OplogRow[] = [];
      const acks: AckResult[] = [];
      const targets: Target[] = [];
      for (const [i, w] of writes.entries()) {
        const id = w.writeId.toLowerCase();
        if (!isNew[i]) {
          // §4 step 4: the ack is the ORIGINAL rev, and is indistinguishable from a first commit.
          acks.push({ writeId: w.writeId, rev: (known.get(id) ?? inBatch.get(id)) as number, duplicate: true });
          continue;
        }
        const rev = next++;
        inBatch.set(id, rev);
        // Resolved now, applied once below: the whole group's nodes work is two statements, not two
        // per write, and it all happens inside the same counter lock either way.
        targets.push(...this.#resolve(w.path, w.op, w.value, rev));
        entries.push({ rev, path: w.path, op: w.op, value: w.value, writeId: id });
        acks.push({ writeId: w.writeId, rev, duplicate: false });
      }

      await this.#applyTargets(c, targets);
      await this.#writeOplog(c, entries);
      return acks;
    });
    this.#fire();
    return results;
  }

  /**
   * §4 step 3: SOLO and counter-lock-FIRST — the lock is taken before the oplog check and held
   * through the commit, which is the ordering that closes the check/commit race between concurrent
   * CAS writes. A mismatch is a normal outcome carrying fresh state, not an error.
   */
  async commitCas(write: CasWrite): Promise<CasResult> {
    await this.#init();
    const id = write.writeId.toLowerCase();
    const result = await this.#tx<CasResult>(async (c) => {
      const counter = await this.#lockCounter(c);

      const known = await this.#priorRevs(c, [id]);
      const prior = known.get(id);
      if (prior !== undefined) return { ok: true, rev: prior, duplicate: true };

      // §4: an expectedRev below the watermark cannot be proven safe — we no longer hold the history
      // that would decide it. Conservative means casFail, never an optimistic commit.
      if (write.expectedRev < counter.prunedThrough) return await this.#casFail(c, write.path, counter.v);

      // Relevance is §3's predicate: an entry at/under this path, or an ancestor of it (a delete of
      // the parent must conflict — which is exactly why this is defined on the oplog, not leaf revs).
      const { rows: conflict } = await c.query(
        `SELECT 1 FROM oplog WHERE rev > $1 AND ${RELEVANT_SQL} LIMIT 1`,
        [write.expectedRev, ancestorsInclusive(write.path), likeDescendants(write.path)],
      );
      if (conflict.length > 0) return await this.#casFail(c, write.path, counter.v);

      const { rows } = await c.query<{ v: string }>(
        'UPDATE rev_counter SET v = v + 1 WHERE shard = 0 RETURNING v',
      );
      const rev = Number((rows[0] as { v: string }).v);
      await this.#applyTargets(c, this.#resolve(write.path, 'put', write.value, rev));
      await this.#writeOplog(c, [{ rev, path: write.path, op: 'put', value: write.value, writeId: id }]);
      return { ok: true, rev, duplicate: false };
    });
    // A failed CAS and a duplicate replay changed nothing; only a real commit is a commit (memory.ts).
    if (result.ok && !result.duplicate) this.#fire();
    return result;
  }

  /** §3 catch-up: entries relevant to `path` (at/under it, or an ancestor of it), ascending. */
  async readCatchup(path: string, sinceRev: number, limit: number): Promise<OplogEntry[]> {
    await this.#init();
    const { rows } = await this.#pool.query<OplogRowDb>(
      `${oplogColumns(this.#schema)} WHERE rev > $1 AND ${RELEVANT_SQL} ORDER BY rev LIMIT $4`,
      [sinceRev, ancestorsInclusive(path), likeDescendants(path), limit],
    );
    return rows.map(toEntry);
  }

  /** §8 dispatcher tail. */
  async readOplogSince(afterRev: number, limit: number): Promise<OplogEntry[]> {
    await this.#init();
    const { rows } = await this.#pool.query<OplogRowDb>(
      `${oplogColumns(this.#schema)} WHERE rev > $1 ORDER BY rev LIMIT $2`,
      [afterRev, limit],
    );
    return rows.map(toEntry);
  }

  /**
   * In-process commit notification, fired after COMMIT returns. The cross-process half is the
   * `NOTIFY` issued inside the same transaction (see #writeOplog); a LISTEN connection to pick it up
   * lands at Gate C. Both are pokes — order comes from re-reading the oplog, never from a callback.
   */
  onCommit(cb: () => void): () => void {
    this.#listeners.add(cb);
    void this.#listen();
    return () => this.#listeners.delete(cb);
  }

  /**
   * §9 retention, both bounds: drop oplog rows older than OPLOG_RETENTION_MS **or** beyond the last
   * OPLOG_RETENTION_REVS — whichever prunes MORE — advancing `pruned_through` in the same
   * transaction as the delete, so no reader can ever see a watermark the rows disagree with.
   *
   * `nodes` is never touched: it is the materialised present, not history. Returns the new watermark.
   */
  async prune(): Promise<number> {
    await this.#init();
    return this.#tx((c) => this.#prune(c, true));
  }

  async close(): Promise<void> {
    this.#closed = true;
    this.#unhookPool();
    this.#unlisten?.();
    this.#unlisten = null;
    if (this.#ownsListener) await this.#listener.close();
    // A borrowed pool belongs to whoever built it — ending it here would take every other tenant's
    // connections down with this one adapter.
    if (this.#ownsPool) await this.#pool.end();
  }

  // ------------------------------------------------------------------ internals

  /** One transaction, one connection. Every adapter method that writes goes through here. */
  async #tx<T>(fn: (c: pg.PoolClient) => Promise<T>): Promise<T> {
    const c = await this.#pool.connect();
    try {
      await this.#learnPid(c);
      await c.query('BEGIN');
      // §5.22 Gate B, and `LOCAL` is the mechanism, not a style choice: a plain `SET` outlives the
      // transaction AND the checkout, so the next tenant to borrow this connection would inherit
      // this one's schema — measured, and it is the silent cross-tenant write this gate exists to
      // make impossible. `SET LOCAL` reverts at COMMIT/ROLLBACK, back to `NO_TENANT_SCHEMA`, which
      // resolves nothing.
      //
      // In the PREAMBLE rather than a pool checkout hook, because a hook is a second place to
      // remember and this must live where the transaction does.
      await c.query(`SET LOCAL search_path = ${this.#schema}`);
      const out = await fn(c);
      await c.query('COMMIT');
      return out;
    } catch (err) {
      await c.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      c.release();
    }
  }

  /**
   * §4's lock ordering. Every write transaction takes this row lock first and holds it to COMMIT, so
   * writes serialize here — WORKLOAD §4: that IS the design, not a bottleneck to engineer around.
   */
  async #lockCounter(c: pg.PoolClient): Promise<{ v: number; prunedThrough: number }> {
    const { rows } = await c.query<{ v: string; pruned_through: string }>(
      'SELECT v, pruned_through FROM rev_counter WHERE shard = 0 FOR UPDATE',
    );
    const row = rows[0] as { v: string; pruned_through: string };
    return { v: Number(row.v), prunedThrough: Number(row.pruned_through) };
  }

  /**
   * §4 step 4's dedup index, read under the counter lock. Keys are lowercased because Postgres
   * normalises `uuid`: to the unique index `A1B2…` and `a1b2…` are one writeId, and a dedup map that
   * disagreed would send a duplicate down the insert path and abort the transaction.
   */
  async #priorRevs(c: pg.PoolClient, writeIds: string[]): Promise<Map<string, number>> {
    const { rows } = await c.query<{ write_id: string; rev: string }>(
      'SELECT write_id, rev FROM oplog WHERE write_id = ANY($1::uuid[])',
      [writeIds.map((w) => w.toLowerCase())],
    );
    return new Map(rows.map((r) => [r.write_id, Number(r.rev)]));
  }

  /**
   * One subtree replacement: the leaves that land at `path`, under `rev`. Resolving a write into
   * these BEFORE touching the database is what lets a whole group go in two statements instead of
   * two per write — see `#applyTargets`.
   */
  #resolve(path: string, op: WriteOp, value: Json, rev: number): Target[] {
    const one = (at: string, v: Json): Target => {
      const flat = flatten(at, v, this.#limits);
      // Validation (§4 step 1) already rejected anything unflattenable; reaching here with an error
      // would be a pipeline bug, and silently storing nothing would hide it.
      if (!flat.ok) throw new Error(`storage received an unvalidated write at "${at}": ${flat.msg}`);
      return { path: at, leaves: flat.leaves, rev };
    };
    // §4: a merge is a child put per key, all under ONE rev — that is what makes deep keys atomic.
    return op === 'merge'
      ? Object.entries(value as { [k: string]: Json }).map(([key, child]) => one(joinPath(path, key), child))
      : [one(path, value)];
  }

  /**
   * Every resolved write of one commit, in a CONSTANT number of statements rather than two per write.
   *
   * The old shape issued a DELETE and an INSERT per write, serially, inside the `rev_counter` lock.
   * A 5-write group was 10 round trips; a 20-key merge is ONE write costing 40, because §4 expands
   * it to a put per key. That multiplier is paid entirely inside the lock, so it is the hold, and
   * the hold is the ceiling.
   *
   * Batching is only sound when no target can disturb another, and one fact gives that: two paths
   * that are both prefixes of the same string are prefixes of one another. Everything this DELETE
   * touches for target A — A's subtree, A's ancestors — is a path A is a prefix of or a prefix of A.
   * So for it to reach anything of B's, A and B would have to be prefix-related, which the guard
   * excludes. With that, all DELETEs may precede all INSERTs and per-write ordering stops mattering.
   *
   * When it does NOT hold, order is the answer and we pay the old cost for that group — and
   * `applyStats` counts it, because a workload that always collided would otherwise pay the old
   * per-write cost forever with nothing to show it. Merges are the case worth naming:
   * `{"a/b": 1, "a": {"z": 2}}` is a LEGAL merge — validate.ts allows deep relative keys — and
   * applied in order it leaves only `p/a/z`, while batched it would leave `p/a/b` too. Merge keys
   * are NOT disjoint by construction, so they take the same check as everything else.
   */
  async #applyTargets(c: pg.PoolClient, targets: Target[]): Promise<void> {
    if (targets.length === 0) return;
    if (targets.length > 1) {
      this.#applyStats.groups++;
      if (!prefixDisjoint(targets.map((t) => t.path))) {
        this.#applyStats.orderedFallbacks++;
        for (const t of targets) await this.#applyTargets(c, [t]);
        return;
      }
    }

    await this.#deleteTargets(c, targets);

    const paths: string[] = [];
    const values: string[] = [];
    const revs: number[] = [];
    for (const t of targets) {
      for (const l of t.leaves) {
        paths.push(l.path);
        values.push(JSON.stringify(l.value));
        revs.push(t.rev);
      }
    }
    if (paths.length === 0) return; // null and {} store nothing (§1)

    // Per-row rev, not one bound value: a group carries a different rev per write.
    await c.query(
      `INSERT INTO nodes (path, value, rev)
       SELECT p, v, r FROM unnest($1::text[], $2::jsonb[], $3::bigint[]) AS t(p, v, r)
           ON CONFLICT (path) DO UPDATE SET value = EXCLUDED.value, rev = EXCLUDED.rev`,
      [paths, values, revs],
    );
  }

  /**
   * Everything at or under each target goes, and so does any scalar sitting at an ancestor —
   * writing `a/b/c` turns a scalar `a/b` into an object.
   *
   * The descendant half is a RANGE, never a LIKE, and that is the whole point of this method.
   * `n.path LIKE t.pat` with `t.pat` a COLUMN cannot use an index at ANY array size — a LIKE
   * prefix becomes an index bound only when the pattern is known at plan time — so the first
   * batched version of this turned the DELETE into a Nested Loop over a Seq Scan of `nodes`:
   * measured 81 ms for a five-write group at 40k rows and 1.5 s at 400k, all of it inside the
   * lock, and growing with the table. `~>=~`/`~<~` are the `text_pattern_ops` operators
   * `nodes_path_pattern` is built for (`TOPNODES_SQL` already leans on them) and they need no
   * escaping: `[p || '/', p || '0')` is EXACTLY p's descendants, because '/' is 0x2F and '0' is
   * 0x30, so no legal path sorts between them. `plans.pgtest.ts` pins the plan.
   */
  async #deleteTargets(c: pg.PoolClient, targets: Target[]): Promise<void> {
    const first = targets[0] as Target;
    // A put at root replaces the whole tree, and root is the one path with no `[lo, hi)`: every
    // path descends from it. It can only ever arrive alone — root is prefix-related to
    // everything, so a group containing it never passes the guard above.
    if (first.path === '') {
      await c.query('DELETE FROM nodes');
      return;
    }

    // `ancestorsInclusive` carries the target itself, and a group of siblings shares nearly all of
    // its ancestors, so the set is not decoration: 2,202 depth-4 targets collapse from 11,010
    // array entries to 7,107.
    const exact = new Set<string>();
    for (const t of targets) for (const a of ancestorsInclusive(t.path)) exact.add(a);

    if (targets.length === 1) {
      // One target needs no join, so both halves stay in ONE statement. This is the path the
      // ordered fallback loops over, once per write inside the lock, and a second round trip there
      // is exactly what it cannot afford.
      await c.query(DELETE_SOLO_SQL, [[...exact], `${first.path}/`, `${first.path}0`]);
      return;
    }

    // Two statements, because the halves cannot share one: an OR across a join condition is the
    // shape that loses the index. The exact half joins nothing, so its worst case is ONE scan of
    // `nodes` — the planner takes it over thousands of index probes and is right to; the
    // descendant half stays index-driven at every size measured, and that is what the test pins.
    await c.query(DELETE_EXACT_SQL, [[...exact]]);
    await c.query(DELETE_RANGE_SQL, [targets.map((t) => `${t.path}/`), targets.map((t) => `${t.path}0`)]);
  }

  /** The oplog half of the same transaction, plus §9 retention and §8's cross-process poke. */
  async #writeOplog(c: pg.PoolClient, entries: OplogRow[]): Promise<void> {
    if (entries.length === 0) return;
    await c.query(
      `INSERT INTO oplog (rev, path, op, value, write_id, ts)
       SELECT r, p, o, v, w, now()
         FROM unnest($1::bigint[], $2::text[], $3::smallint[], $4::jsonb[], $5::uuid[]) AS t(r, p, o, v, w)`,
      [
        entries.map((e) => e.rev),
        entries.map((e) => e.path),
        entries.map((e) => OP_CODE[e.op]),
        // JSON null is a delete and must survive as jsonb 'null'; a JS null parameter would be SQL NULL.
        entries.map((e) => JSON.stringify(e.value)),
        entries.map((e) => e.writeId),
      ],
    );
    await this.#prune(c, false);
    // Queued until COMMIT by Postgres, so a listener is never woken for a write it cannot yet read.
    // §5.22 Gate C: the tenant's OWN channel. Unqualified, this woke every other tenant's
    // dispatcher on the same Postgres database — LISTEN/NOTIFY is per-database, not per-schema.
    // `NOTIFY` takes an identifier, so the constructor's schema-name guard is what makes this safe;
    // there is deliberately no second check beside it.
    await c.query(`NOTIFY ${commitChannel(this.#schema)}`);
  }

  /**
   * The watermark advance, shared by the write path and `prune()`.
   *
   * Every commit applies the rev-COUNT bound inline, because memory.ts prunes on record and the
   * conformance suite pins that (three writes at OPLOG_RETENTION_REVS=2 leave pruned_through at 1).
   * The TIME bound is only for the timer: it needs `max(rev)` over a `ts` range, which no index
   * serves, and that is not a price a write should pay 500k times to find nothing.
   */
  async #prune(c: pg.PoolClient, timeBound: boolean): Promise<number> {
    const terms = ['pruned_through', 'v - $1'];
    const params: unknown[] = [this.#limits.OPLOG_RETENTION_REVS];
    if (timeBound) {
      params.push(this.#limits.OPLOG_RETENTION_MS / 1000);
      terms.push(
        `coalesce((SELECT max(rev) FROM oplog WHERE ts < now() - make_interval(secs => $2::double precision)), 0)`,
      );
    }
    const { rows } = await c.query<{ pruned_through: string }>(
      `UPDATE rev_counter SET pruned_through = GREATEST(${terms.join(', ')})
        WHERE shard = 0 RETURNING pruned_through`,
      params,
    );
    const through = Number((rows[0] as { pruned_through: string }).pruned_through);
    if (through > 0) await c.query('DELETE FROM oplog WHERE rev <= $1', [through]);
    return through;
  }

  /** §4: a mismatch carries fresh state — read inside the same transaction, so it is consistent. */
  async #casFail(c: pg.PoolClient, path: string, head: number): Promise<CasResult> {
    const { rows } = await c.query<{ leaves: Leaf[] }>(
      `SELECT ${LEAVES('n')} AS leaves FROM nodes n WHERE ${atOrUnder('n.path')}`,
      [path, likeDescendants(path)],
    );
    return { ok: false, rev: head, value: unflatten(path, (rows[0] as { leaves: Leaf[] }).leaves) };
  }

  /**
   * §8's "NOTIFY-triggered poll", now through the shared `CommitListener`: a connection that is
   * never a pool one — it must not be handed to a transaction — waiting for another process's
   * commit on THIS tenant's channel. The notification is a poke and carries nothing; the dispatcher
   * learns WHAT changed by re-reading the oplog.
   *
   * Idempotent, and it has to be: `onCommit` calls it on every subscription.
   */
  async #listen(): Promise<void> {
    if (this.#unlisten || this.#closed) return;
    this.#unlisten = () => undefined; // claim the slot before the await, or two callers race it
    const stop = await this.#listener.listen(commitChannel(this.#schema), (pid) => {
      // Our own commits already woke the local listeners synchronously (`#fire` after COMMIT), so a
      // NOTIFY from one of our own backends must not wake them twice. `-1` is the reconnect poke,
      // which belongs to nobody and must always land.
      if (!this.#ownPids.has(pid)) this.#fire();
    });
    // `onCommit` calls this UN-AWAITED, so `close()` can land inside the await above — and when it
    // does it calls the placeholder, which stops nothing, and this line would then hand a live
    // subscription to a CLOSED adapter. Measured before this check: the closed adapter still fired
    // on the next commit, and the shared listener held its callback (and through it the adapter)
    // for as long as the listener lived — the same pin `8d3be3a` was about, one `await` higher.
    if (this.#closed) return stop();
    this.#unlisten = stop;
  }

  /**
   * Records this connection's backend PID, once, so #listen can tell our own commits from another
   * gateway's. It runs HERE — inside the transaction helper, where we hold the connection
   * exclusively — and not from the pool's `connect` event: a query issued there overlaps the first
   * query the borrower sends on the same client, and node-postgres does not support two in flight
   * ("Calling client.query() when the client is already executing a query"). The overlap resolves
   * fine most of the time and then, about one run in six, leaves a promise that never settles and a
   * gateway that will not shut down.
   */
  async #learnPid(c: pg.PoolClient): Promise<void> {
    const tracked = c as TrackedClient;
    if (tracked[BACKEND_PID] !== undefined) return;
    const { rows } = await c.query<{ pid: number }>('SELECT pg_backend_pid() AS pid');
    const pid = Number((rows[0] as { pid: number }).pid);
    tracked[BACKEND_PID] = pid;
    this.#ownPids.add(pid);
  }

  #fire(): void {
    for (const cb of this.#listeners) cb();
  }

  /**
   * Applies the schema and settles the epoch, once per adapter. A fresh store gets a NEW random
   * generation (§2, memory.ts precedent: a store that comes up without its past is exactly the reset
   * the epoch announces); an existing one keeps what is on disk — that is the whole ON CONFLICT.
   */
  #init(): Promise<number> {
    this.#ready ??= (async () => {
      const c = await this.#pool.connect();
      try {
        await c.query('BEGIN');
        // Two adapters racing `CREATE TABLE IF NOT EXISTS` against one schema is a known Postgres
        // deadlock; one advisory lock costs nothing and removes the whole class.
        //
        // TWO locks now, and the ORDER is load-bearing: control first, tenant second, always. Every
        // adapter on the shard takes the SAME control lock (one name) and its OWN tenant lock, so a
        // fixed order means no two adapters can hold one and want the other. Reversed, N adapters
        // starting together would be a deadlock waiting for the day N is large — and this is Gate
        // D's precondition, so N is exactly what is coming.
        await c.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`rtdb-schema:${this.#control}`]);
        await c.query(`CREATE SCHEMA IF NOT EXISTS ${this.#control}`);
        await c.query(CONTROL_SQL(this.#control));
        // §5.24 Gate C, inside the SAME advisory lock and transaction as the table it alters — a
        // migration that races N adapters starting together is the one this file already paid for.
        await c.query(CONTROL_QUOTA_SQL(this.#control));
        await c.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`rtdb-schema:${this.#schema}`]);
        await c.query(`CREATE SCHEMA IF NOT EXISTS ${this.#schema}`);
        // The SEVENTH site, and the one exception to "qualify it": `SCHEMA_SQL` is a FILE, and every
        // statement in it is unqualified. Qualifying it means interpolating a schema name into
        // `schema.sql`, which stops it being SQL you can run. `SET LOCAL` costs one statement, once
        // per adapter, at startup — the round-trip argument that rules it out for the hot reads does
        // not reach here. Measured without it: `no schema has been selected to create in`.
        await c.query(`SET LOCAL search_path = ${this.#schema}`);
        await c.query(SCHEMA_SQL);
        await c.query(
          `INSERT INTO rev_counter (shard, v, epoch, pruned_through) VALUES (0, 0, $1, 0)
             ON CONFLICT (shard) DO NOTHING`,
          [randomInt(1, 2 ** 31)],
        );
        const { rows } = await c.query<{ epoch: string }>('SELECT epoch FROM rev_counter WHERE shard = 0');
        await c.query('COMMIT');
        return Number((rows[0] as { epoch: string }).epoch);
      } catch (err) {
        await c.query('ROLLBACK').catch(() => undefined);
        this.#ready = null; // a failed apply must not latch — the next call retries
        throw err;
      } finally {
        c.release();
      }
    })();
    return this.#ready;
  }

  /** BIGINT arrives as a string over the wire; every counter read goes through here. */
  async #counter(column: 'v' | 'pruned_through'): Promise<number> {
    await this.#init();
    const { rows } = await this.#pool.query<Record<string, string>>(
      `SELECT ${column} AS n FROM ${this.#schema}.rev_counter WHERE shard = 0`,
    );
    return Number((rows[0] as { n: string }).n);
  }
}

/**
 * §5.22 Gate D shart (A) — every connection this gateway makes to Postgres, on ONE pool and ONE
 * listener, the DEFAULT tenant included.
 *
 * It lives here rather than inline in `main.ts` for the same reason `createPool` does: it is the
 * only place that knows the order these three things have to be built in, and that order is the
 * whole of the condition. Built the other way round — `storageFromEnv()` first, shared pool after —
 * the default tenant keeps a private pool of 10 and a `CommitListener` of its own, so a
 * multi-tenant gateway costs `sharedPoolSize(N) + 1` PLUS 11, and `rtdb_pg_pool_waiting` cannot see
 * the pool that is actually serving today's whole production.
 *
 * The probe is the awkward part and it is not avoidable: a `pg.Pool`'s `max` is fixed at
 * construction, so N has to be read from OUTSIDE the pool that N sizes. One connection, opened and
 * closed before the real pool exists.
 */
export async function openSharedTenancy(opts: {
  url: string;
  limits?: Limits;
  /** The default tenant's schema — what this gateway was already serving. */
  schema?: string;
  controlSchema?: string;
}): Promise<{
  storage: PostgresStorage;
  tenantStorage: (db: string) => PostgresStorage;
  pool: pg.Pool;
  declared: string[];
  /** What the pool was sized to, so the boot line can say it without re-deriving it. */
  max: number;
  /**
   * Ends the pool and the listener. It has to exist: with both of them BORROWED, no adapter's own
   * `close()` touches them any more — `PostgresStorage#close` deliberately leaves a pool it did not
   * build alone, or one tenant closing would take every other tenant's connections with it. So
   * whoever built them closes them, which is the same rule, one level up.
   */
  close(): Promise<void>;
}> {
  const base = {
    url: opts.url,
    ...(opts.limits ? { limits: opts.limits } : {}),
    ...(opts.controlSchema ? { controlSchema: opts.controlSchema } : {}),
  };
  const probe = new PostgresStorage({ ...base, schema: opts.schema ?? 'public', poolMax: 1 });
  let declared: string[];
  try {
    declared = await probe.listDeclared();
  } finally {
    await probe.close();
  }
  /**
   * N from the REGISTRY, not from `topNodes()`: that one unions the declared names with every
   * top-level key that has data, so a shard holding 30 raw namespaces under 3 declared databases
   * sized the pool for 30 — RDS connections spent on `WritePipeline` chains that do not exist.
   * `+ 1` is the default tenant, which is a database this gateway serves and is never declared.
   *
   * Read once, because a pool cannot be resized: a database declared after this shares the reads
   * headroom until the next restart, which is what `rtdb_pg_pool_waiting` exists to make visible.
   */
  const max = sharedPoolSize(declared.length + 1);
  const pool = createPool(opts.url, max);
  const listener = new CommitListener(opts.url);
  const make = (schema: string): PostgresStorage =>
    new PostgresStorage({ ...base, schema, pool, listener });
  return {
    storage: make(opts.schema ?? 'public'),
    tenantStorage: make,
    pool,
    declared,
    max,
    close: async () => {
      await listener.close();
      await pool.end();
    },
  };
}
