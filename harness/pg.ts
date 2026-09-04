import { randomBytes } from 'node:crypto';
import pg from 'pg';

/**
 * Where the Postgres-backed suites connect. Shared by the storage tests, the gateway harness and
 * the chaos runner so there is exactly one answer to "which server?".
 */
export const PG_URL = process.env['RTDB_PG_URL'] ?? 'postgres://localhost:5432/postgres';

export const isPostgres = (): boolean => (process.env['RTDB_STORAGE'] ?? 'memory') === 'postgres';

/** A schema name nothing else will claim, so one database can host many independent shards. */
export const uniqueSchema = (prefix = 't'): string => `${prefix}_${randomBytes(4).toString('hex')}`;

/** Drops a schema a test created. Never called with a name a test did not mint itself. */
export async function dropSchema(schema: string): Promise<void> {
  if (!/^[a-z][a-z0-9_]*$/.test(schema)) throw new Error(`illegal schema name: ${schema}`);
  const c = new pg.Client({ connectionString: PG_URL });
  await c.connect();
  try {
    await c.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
  } finally {
    await c.end();
  }
}
