import { randomBytes } from 'node:crypto';
import pg from 'pg';
import { PG_URL } from '../../harness/pg.ts';
import type { Limits } from '../../src/protocol/limits.ts';
import { PostgresStorage } from '../../src/storage/postgres.ts';

/** Points at an existing database used ONLY to create and drop the test ones (never written to). */
export const ADMIN_URL = PG_URL;

const urlFor = (name: string): string => {
  const u = new URL(ADMIN_URL);
  u.pathname = `/${name}`;
  return u.href;
};

export interface TestDatabase {
  name: string;
  url: string;
  /** An adapter of its own, in a schema of its own — many independent stores in one database. */
  make(limits?: Limits, schema?: string): PostgresStorage;
  /** A raw client on the same database, for arranging or inspecting rows directly. */
  client(schema?: string): Promise<pg.Client>;
  drop(): Promise<void>;
}

/**
 * Forces the pool to establish `n` connections before a race starts.
 *
 * Without this a "concurrent" test is quietly serial: the pool opens connections lazily, so the
 * first racer gets the one warm connection and commits (~3ms) while the others are still doing TCP
 * and auth (~3ms each). The winner is decided by handshake latency, not by the lock ordering, and
 * the test passes just as happily with the lock removed. Ask me how I know.
 */
export const warmPool = (s: PostgresStorage, n: number): Promise<unknown> =>
  Promise.all(Array.from({ length: n }, () => s.head()));

/**
 * WP4 test isolation: ONE database per test FILE (created here, dropped in the file's `after`), and
 * one Postgres SCHEMA per adapter inside it, so the conformance suite's `fresh()` returns a genuinely
 * empty store every time without paying for a CREATE DATABASE per test. Tests only ever create and
 * drop databases they named themselves (WORKLOAD §2).
 */
export async function createDatabase(label: string): Promise<TestDatabase> {
  if (!/^[a-z][a-z0-9_]*$/.test(label)) throw new Error(`illegal database label: ${label}`);
  const name = `rtdb_${label}_${randomBytes(4).toString('hex')}`;

  const admin = new pg.Client({ connectionString: ADMIN_URL });
  await admin.connect();
  try {
    await admin.query(`CREATE DATABASE ${name}`);
  } finally {
    await admin.end();
  }

  const open: { close(): Promise<void> }[] = [];
  let n = 0;

  return {
    name,
    url: urlFor(name),
    make(limits?: Limits, schema?: string): PostgresStorage {
      const store = new PostgresStorage({
        url: urlFor(name),
        schema: schema ?? `s${++n}`,
        poolMax: 16, // the concurrency tests run a dozen transactions at once
        ...(limits ? { limits } : {}),
      });
      open.push(store);
      return store;
    },
    async client(schema = 'public'): Promise<pg.Client> {
      const c = new pg.Client({ connectionString: urlFor(name), options: `-c search_path=${schema}` });
      await c.connect();
      open.push({ close: () => c.end() });
      return c;
    },
    async drop(): Promise<void> {
      // Pools must go first: Postgres refuses to drop a database with live sessions. FORCE is the
      // belt to that suspenders — a leaked connection must not wedge the whole test run.
      await Promise.allSettled(open.map((o) => o.close()));
      const a = new pg.Client({ connectionString: ADMIN_URL });
      await a.connect();
      try {
        await a.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
      } finally {
        await a.end();
      }
    },
  };
}
