import assert from 'node:assert/strict';
import test, { after, type TestContext } from 'node:test';
import { ancestorsInclusive } from '../../src/protocol/path.ts';
import { likeDescendants, RELEVANT_SQL, TOPNODES_SQL } from '../../src/storage/postgres.ts';
import { createDatabase } from './helper.ts';

/**
 * Gate C's evidence: §8 says both relevance shapes are index-served, and this asserts it against a
 * table big enough for the planner to have a choice — with PARAMETERS, the way the adapter runs
 * them, not with literals folded in.
 */
const db = await createDatabase('plans');
after(() => db.drop());

const SCHEMA = 'plans';
const ROWS = 40_000;
const DEEP = 'MPK_1010/1474396/game/round/7/players/u_42';

const store = db.make(undefined, SCHEMA);
await store.head(); // applies the schema
const c = await db.client(SCHEMA);

// A shard with depth and breadth: 40k oplog entries, one leaf each, plus the deep path's own
// ancestors so the ancestor shape has real rows to find.
await c.query(
  `INSERT INTO oplog (rev, path, op, value, write_id, ts)
   SELECT i,
          'MPK_' || (i % 500) || '/' || (i % 977) || '/game/round/' || (i % 13) || '/players/u_' || i,
          0, to_jsonb(i), gen_random_uuid(), now()
     FROM generate_series(1, $1) i`,
  [ROWS],
);
await c.query(
  `INSERT INTO oplog (rev, path, op, value, write_id, ts)
   SELECT $1 + row_number() OVER (), p, 0, '1'::jsonb, gen_random_uuid(), now()
     FROM unnest($2::text[]) AS t(p)`,
  [ROWS, ancestorsInclusive(DEEP)],
);
await c.query(
  `INSERT INTO nodes (path, value, rev)
   SELECT 'MPK_' || (i % 500) || '/' || (i % 977) || '/leaf', to_jsonb(i), i FROM generate_series(1, $1) i
       ON CONFLICT (path) DO NOTHING`,
  [ROWS],
);
await c.query('ANALYZE oplog, nodes');

interface PlanNode {
  'Node Type': string;
  'Relation Name'?: string;
  'Index Name'?: string;
  Plans?: PlanNode[];
}

const walk = (n: PlanNode, out: PlanNode[] = []): PlanNode[] => {
  out.push(n);
  for (const child of n.Plans ?? []) walk(child, out);
  return out;
};

async function plan(t: TestContext, label: string, sql: string, values: unknown[]): Promise<PlanNode[]> {
  const { rows } = await c.query<{ 'QUERY PLAN': [{ Plan: PlanNode; 'Execution Time': number }] }>(
    `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`,
    values,
  );
  const top = (rows[0] as { 'QUERY PLAN': [{ Plan: PlanNode; 'Execution Time': number }] })['QUERY PLAN'][0];
  const nodes = walk(top.Plan);
  t.diagnostic(
    `${label}: ${nodes.map((n) => n['Index Name'] ?? `${n['Node Type']}${n['Relation Name'] ? ` on ${n['Relation Name']}` : ''}`).join(' <- ')} — ${top['Execution Time'].toFixed(3)}ms`,
  );
  return nodes;
}

const assertNoSeqScan = (nodes: PlanNode[], relation: string, label: string): void => {
  const seq = nodes.filter((n) => n['Node Type'].includes('Seq Scan') && n['Relation Name'] === relation);
  assert.deepEqual(seq, [], `${label} must not sequentially scan ${relation} at ${ROWS} rows`);
};

test('§3 catch-up: the relevance query is index-served at depth', async (t) => {
  const nodes = await plan(
    t,
    'readCatchup',
    `SELECT rev, path, op, value, write_id, ts FROM oplog WHERE rev > $1 AND ${RELEVANT_SQL} ORDER BY rev LIMIT $4`,
    [1, ancestorsInclusive(DEEP), likeDescendants(DEEP), 501],
  );
  assertNoSeqScan(nodes, 'oplog', 'catch-up');
  const indexes = nodes.map((n) => n['Index Name']).filter(Boolean);
  assert.ok(indexes.length > 0, `catch-up used no index at all: ${nodes.map((n) => n['Node Type']).join(', ')}`);
});

test('§4 CAS: the same relevance query, as the conflict check runs it', async (t) => {
  const nodes = await plan(
    t,
    'CAS conflict',
    `SELECT 1 FROM oplog WHERE rev > $1 AND ${RELEVANT_SQL} LIMIT 1`,
    [1, ancestorsInclusive(DEEP), likeDescendants(DEEP)],
  );
  assertNoSeqScan(nodes, 'oplog', 'the CAS conflict check');
});

test('the descendant half alone (LIKE) uses the text_pattern_ops index', async (t) => {
  const nodes = await plan(
    t,
    'descendant only',
    `SELECT rev FROM oplog WHERE rev > $1 AND path LIKE $2 ESCAPE '\\' ORDER BY rev LIMIT 500`,
    [1, likeDescendants('MPK_1010/1474396')],
  );
  assertNoSeqScan(nodes, 'oplog', 'the descendant shape');
  assert.ok(
    nodes.some((n) => n['Index Name'] === 'oplog_path_pattern_rev'),
    `the descendant shape must use oplog_path_pattern_rev, got ${nodes.map((n) => n['Index Name']).join(', ')}`,
  );
});

test('the ancestor half alone (= ANY) uses the (path, rev) index', async (t) => {
  const nodes = await plan(
    t,
    'ancestor only',
    `SELECT rev FROM oplog WHERE rev > $1 AND path = ANY($2::text[]) ORDER BY rev LIMIT 500`,
    [1, ancestorsInclusive(DEEP)],
  );
  assertNoSeqScan(nodes, 'oplog', 'the ancestor shape');
  assert.ok(
    nodes.some((n) => n['Index Name']?.startsWith('oplog_path')),
    `the ancestor shape must use an oplog path index, got ${nodes.map((n) => n['Index Name']).join(', ')}`,
  );
});

test('§8 dispatcher tail: rev > $1 walks the primary key', async (t) => {
  const nodes = await plan(t, 'readOplogSince', `SELECT rev FROM oplog WHERE rev > $1 ORDER BY rev LIMIT $2`, [
    ROWS - 10,
    500,
  ]);
  assertNoSeqScan(nodes, 'oplog', 'the dispatcher tail');
});

test('§3 snapshot: the subtree read is index-served on nodes', async (t) => {
  const nodes = await plan(
    t,
    'readSnapshot',
    `SELECT count(*) FROM nodes n WHERE (n.path = $1 OR n.path LIKE $2 ESCAPE '\\')`,
    ['MPK_1010/1474396', likeDescendants('MPK_1010/1474396')],
  );
  assertNoSeqScan(nodes, 'nodes', 'the snapshot read');
});

// ---------------------------------------------------------------- §5.6 the sidebar's skip scan

test('topNodes SKIPS along the path index instead of scanning every leaf (§5.6)', async (t) => {
  // The naive answer — SELECT DISTINCT split_part(path,'/',1) — is a sequential scan of the whole
  // shard for a result that is a dozen strings, and it gets worse with every leaf ever written.
  // This pins the shape the way the tests above pin RELEVANT_SQL: a plan, on a table big enough for
  // the planner to have a real choice.
  const nodes = await plan(t, 'topNodes skip scan', TOPNODES_SQL, []);
  assertNoSeqScan(nodes, 'nodes', 'topNodes');
  const indexed = nodes.filter((n) => n['Node Type'].includes('Index') && n['Relation Name'] === 'nodes');
  assert.ok(indexed.length > 0, 'topNodes must reach `nodes` through an index, not a scan');

  // And the naive form, for the contrast the comment claims — this one MAY seq scan, and does.
  const naive = await plan(t, 'naive DISTINCT (not shipped)', `SELECT DISTINCT split_part(path, '/', 1) FROM nodes`, []);
  const naiveSeq = naive.filter((n) => n['Node Type'].includes('Seq Scan') && n['Relation Name'] === 'nodes');
  assert.equal(naiveSeq.length, 1, 'the shape we did NOT ship is the seq scan this test exists to avoid');
});

test('topNodes finds ADJACENT namespaces — ns0 beside ns1 and ns10 (§5.6)', async (t) => {
  // The boundary after a namespace is `name || '0'`, and `>=` vs `>` can ONLY be told apart by a row
  // sitting EXACTLY on one. The first version of this fixture could not: it had zz0/zz1/zz10/zz2 but
  // no `zz`, so no boundary ever equalled an existing path and the test passed with `>` — a pin that
  // pinned nothing, found by the mentor pass flipping the operator and watching it stay green.
  //
  // `zz/a` is what makes it real: the walk now visits namespace `zz`, whose boundary is exactly
  // `zz0`, which is where the bare `zz0` row sits. With `>` that row is skipped and the namespace
  // `zz0` vanishes — and a sidebar with one name silently missing still looks like a working sidebar.
  await c.query(`INSERT INTO nodes (path, value, rev) VALUES
      ('zz/a', '1'::jsonb, 1),
      ('zz0', '1'::jsonb, 1), ('zz1/a', '1'::jsonb, 1), ('zz10/b/c', '1'::jsonb, 1), ('zz2', '1'::jsonb, 1)
    ON CONFLICT (path) DO NOTHING`);
  t.after(() => c.query(`DELETE FROM nodes WHERE path IN ('zz/a','zz0','zz1/a','zz10/b/c','zz2')`));

  const { rows } = await c.query<{ name: string }>(TOPNODES_SQL);
  const found = rows.map((r) => r.name);
  // Report only the neighbourhood. The fixture also holds 500 MPK_* namespaces, and dumping them
  // into the failure message buries the one fact the reader needs under six kilobytes of noise.
  const near = found.filter((n) => n.startsWith('zz'));
  for (const want of ['zz', 'zz0', 'zz1', 'zz10', 'zz2']) {
    assert.ok(found.includes(want), `topNodes lost the namespace "${want}" — of the zz* group it found ${JSON.stringify(near)}`);
  }
  assert.equal(new Set(found).size, found.length, 'no namespace may be reported twice');
});

test('topNodes compares in BYTE order, whatever the server collation is (§5.6)', async (t) => {
  // The tooth for the bug that reached production. The walk is only correct if every path under a
  // namespace sorts below `ns || '0'` — true of byte order, because `/` is 0x2F and `0` is 0x30.
  //
  // Written with a plain `>=` it passed every local test, because macOS's en_US.UTF-8 happens to
  // behave byte-wise. RDS's en_US.UTF-8 — SAME NAME, glibc — ignores punctuation at the primary
  // level, so there `'MPK_1010/a' >= 'MPK_10100'` is TRUE, the recursion re-enters the namespace it
  // just left, and it never terminates. It span for four and a half minutes on the production shard.
  //
  // No correctness fixture could have caught it: the fixture runs against THIS database, and this
  // database agrees with the byte order. What is portable is the OPERATOR, so that is what is pinned.
  const { rows } = await c.query<{ pattern_op: boolean; byte_order: boolean; plain_op: boolean }>(
    `SELECT ('MPK_1010/a' ~>=~ 'MPK_10100')                                        AS pattern_op,
            (convert_to('MPK_1010/a','UTF8') >= convert_to('MPK_10100','UTF8'))    AS byte_order,
            ('MPK_1010/a' >= 'MPK_10100')                                          AS plain_op`,
  );
  const r = rows[0] as { pattern_op: boolean; byte_order: boolean; plain_op: boolean };
  assert.equal(r.pattern_op, r.byte_order, '~>=~ must agree with byte order on every platform');
  assert.equal(r.byte_order, false, 'a path under a namespace must sort BELOW that namespace\'s boundary');
  t.diagnostic(`this server's plain >= says ${r.plain_op}; RDS says true, which is the whole bug`);

  // And the query must not quietly go back to a collation-dependent comparison.
  assert.match(TOPNODES_SQL, /~>=~/, 'the boundary comparison must use the byte-ordered operator');
  assert.match(TOPNODES_SQL, /USING ~<~/, 'the ordering must be byte-ordered too, or LIMIT 1 picks the wrong row');
  assert.doesNotMatch(TOPNODES_SQL, /path >= /, 'a plain >= on path is the bug that hung production');
  assert.match(TOPNODES_SQL, /depth < \d+/, 'the depth bound is what turns a future cycle into a truncated list');
});
