import { randomInt } from 'node:crypto';
import { readFileSync } from 'node:fs';
import pg from 'pg';
import { DEFAULT_LIMITS, type Limits } from '../protocol/limits.ts';
import { ancestorsInclusive, joinPath } from '../protocol/path.ts';
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

/** `oplog.op` is a SMALLINT (§8); these two values are the whole mapping. */
export const OP_CODE = { put: 0, merge: 1 } as const;

const OP_NAME = ['put', 'merge'] as const satisfies readonly WriteOp[];

const OPLOG_COLUMNS = 'SELECT rev, path, op, value, write_id, ts FROM oplog';

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

/** "`col` is at or under the path in $1" — reading a snapshot, and keeping `nodes` prefix-free. */
const atOrUnder = (col: string): string => `(${col} = $1 OR ${col} LIKE $2 ESCAPE '\\')`;

/**
 * §3 relevance over the oplog: an entry at/under the path ($3, the escaped descendant pattern), or
 * an ancestor of it ($2, the <=33 expanded ancestor paths). §8 gives one index for each half, and
 * `plans.pgtest.ts` EXPLAINs this exact string to keep it that way.
 */
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
export const TOPNODES_SQL = `
WITH RECURSIVE ns AS (
  SELECT 1 AS depth,
         (SELECT split_part(path, '/', 1) FROM nodes ORDER BY path USING ~<~ LIMIT 1) AS name
  UNION ALL
  SELECT ns.depth + 1,
         (SELECT split_part(n.path, '/', 1) FROM nodes n
           WHERE n.path ~>=~ (ns.name || '0') ORDER BY n.path USING ~<~ LIMIT 1)
    FROM ns WHERE ns.name IS NOT NULL AND ns.depth < 5000
)
SELECT name FROM ns WHERE name IS NOT NULL ORDER BY name`;

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
  poolMax?: number;
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
  readonly #schema: string;
  /** Gate B's commitGroup flattens with these (§9). */
  readonly #limits: Limits;
  /** Resolves to the epoch; also the "schema is applied" latch. Every public method awaits it. */
  #ready: Promise<number> | null = null;
  readonly #listeners = new Set<() => void>();
  readonly #url: string;
  /**
   * The backend PIDs of our OWN pool connections. Our commits already woke the local listeners
   * synchronously, so their NOTIFY must not wake them a second time — §8's poke is for the gateways
   * that did NOT do the write. Filtering on the sender's PID keeps the payload contentless, which
   * is the ruling (WORKLOAD §4).
   */
  readonly #ownPids = new Set<number>();
  #listenClient: pg.Client | null = null;

  constructor(opts: PostgresOptions) {
    this.#url = opts.url;
    this.#schema = opts.schema ?? 'public';
    if (!/^[a-z_][a-z0-9_]*$/.test(this.#schema)) {
      // The schema name is interpolated into DDL; nothing but an identifier may reach that string.
      throw new Error(`illegal schema name: ${this.#schema}`);
    }
    this.#limits = opts.limits ?? DEFAULT_LIMITS;
    this.#pool = new pg.Pool({
      connectionString: opts.url,
      max: opts.poolMax ?? 10,
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
      options: `-c search_path=${this.#schema}`,
    });
    // A connection that goes away takes its PID with it: Postgres reuses PIDs, and a stale one here
    // would silently swallow a real notification from another gateway.
    this.#pool.on('remove', (c) => {
      const pid = (c as TrackedClient)[BACKEND_PID];
      if (pid !== undefined) this.#ownPids.delete(pid);
    });
  }

  head(): Promise<number> {
    return this.#counter('v');
  }

  /** §2: the generation is read once at startup and never moves while the store lives. */
  epoch(): Promise<number> {
    return this.#init();
  }

  /** §5.6's sidebar. Skip scan over the path index — see TOPNODES_SQL for why, and why not counts. */
  async topNodes(): Promise<string[]> {
    const { rows } = await this.#pool.query<{ name: string }>(TOPNODES_SQL);
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
      `SELECT (SELECT v FROM rev_counter WHERE shard = 0) AS rev,
              (SELECT ${LEAVES('n')} FROM nodes n WHERE ${atOrUnder('n.path')}) AS leaves`,
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
      for (const [i, w] of writes.entries()) {
        const id = w.writeId.toLowerCase();
        if (!isNew[i]) {
          // §4 step 4: the ack is the ORIGINAL rev, and is indistinguishable from a first commit.
          acks.push({ writeId: w.writeId, rev: (known.get(id) ?? inBatch.get(id)) as number, duplicate: true });
          continue;
        }
        const rev = next++;
        inBatch.set(id, rev);
        await this.#apply(c, w.path, w.op, w.value, rev);
        entries.push({ rev, path: w.path, op: w.op, value: w.value, writeId: id });
        acks.push({ writeId: w.writeId, rev, duplicate: false });
      }

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
      await this.#apply(c, write.path, 'put', write.value, rev);
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
      `${OPLOG_COLUMNS} WHERE rev > $1 AND ${RELEVANT_SQL} ORDER BY rev LIMIT $4`,
      [sinceRev, ancestorsInclusive(path), likeDescendants(path), limit],
    );
    return rows.map(toEntry);
  }

  /** §8 dispatcher tail. */
  async readOplogSince(afterRev: number, limit: number): Promise<OplogEntry[]> {
    await this.#init();
    const { rows } = await this.#pool.query<OplogRowDb>(
      `${OPLOG_COLUMNS} WHERE rev > $1 ORDER BY rev LIMIT $2`,
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
    const listener = this.#listenClient;
    this.#listenClient = null;
    await listener?.end().catch(() => undefined);
    await this.#pool.end();
  }

  // ------------------------------------------------------------------ internals

  /** One transaction, one connection. Every adapter method that writes goes through here. */
  async #tx<T>(fn: (c: pg.PoolClient) => Promise<T>): Promise<T> {
    const c = await this.#pool.connect();
    try {
      await this.#learnPid(c);
      await c.query('BEGIN');
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

  /** §4: a merge is a child put per key, all under ONE rev — that is what makes deep keys atomic. */
  async #apply(c: pg.PoolClient, path: string, op: WriteOp, value: Json, rev: number): Promise<void> {
    if (op === 'merge') {
      for (const [key, child] of Object.entries(value as { [k: string]: Json })) {
        await this.#put(c, joinPath(path, key), child, rev);
      }
      return;
    }
    await this.#put(c, path, value, rev);
  }

  /** Replace the subtree at `path`, keeping the leaf set prefix-free in BOTH directions. */
  async #put(c: pg.PoolClient, path: string, value: Json, rev: number): Promise<void> {
    const flat = flatten(path, value, this.#limits);
    // Validation (§4 step 1) already rejected anything unflattenable; reaching here with an error
    // would be a pipeline bug, and silently storing nothing would hide it.
    if (!flat.ok) throw new Error(`storage received an unvalidated write at "${path}": ${flat.msg}`);

    // Everything at or under `path` goes, and so does any scalar sitting at an ancestor — writing
    // `a/b/c` turns a scalar `a/b` into an object.
    await c.query(
      `DELETE FROM nodes WHERE ${atOrUnder('path')} OR path = ANY($3::text[])`,
      [path, likeDescendants(path), ancestorsInclusive(path).filter((a) => a !== path)],
    );
    if (flat.leaves.length === 0) return; // null and {} store nothing (§1)

    await c.query(
      `INSERT INTO nodes (path, value, rev)
       SELECT p, v, $3 FROM unnest($1::text[], $2::jsonb[]) AS t(p, v)
           ON CONFLICT (path) DO UPDATE SET value = EXCLUDED.value, rev = EXCLUDED.rev`,
      [flat.leaves.map((l) => l.path), flat.leaves.map((l) => JSON.stringify(l.value)), rev],
    );
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
    await c.query('NOTIFY rtdb_commit');
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
   * §8's "NOTIFY-triggered poll": one dedicated connection (never a pool one — it must not be handed
   * to a transaction) waiting for another process's commit. The notification is a poke and carries
   * nothing; the dispatcher learns WHAT changed by re-reading the oplog.
   *
   * ponytail: a dropped LISTEN connection is not re-established. In v1 one process both commits and
   * dispatches, so the local listeners carry every wake-up and this connection is redundant; Phase
   * 5's second gateway is what makes a reconnect loop here worth writing.
   */
  async #listen(): Promise<void> {
    if (this.#listenClient) return;
    const c = new pg.Client({ connectionString: this.#url });
    this.#listenClient = c;
    c.on('error', () => undefined); // a dead poke connection must never take the process with it
    c.on('notification', (msg) => {
      if (!this.#ownPids.has(msg.processId)) this.#fire();
    });
    try {
      await c.connect();
      await c.query('LISTEN rtdb_commit');
    } catch {
      this.#listenClient = null;
    }
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
        await c.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`rtdb-schema:${this.#schema}`]);
        await c.query(`CREATE SCHEMA IF NOT EXISTS ${this.#schema}`);
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
      `SELECT ${column} AS n FROM rev_counter WHERE shard = 0`,
    );
    return Number((rows[0] as { n: string }).n);
  }
}
