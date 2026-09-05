/**
 * Empty the shard and announce a NEW GENERATION. Run by the OPERATOR, deliberately.
 *
 *   RTDB_PG_URL=... node --import tsx scripts/reset-shard.ts          # show what would go, change nothing
 *   RTDB_PG_URL=... node --import tsx scripts/reset-shard.ts --yes    # do it
 *
 * This is not a migration or a prune. It throws away every node and the whole oplog, and it is
 * meant for a shard whose contents were only ever test traffic.
 *
 * THE EPOCH IS THE POINT, not a detail. §2 makes the generation persistent precisely so that a
 * store which comes up without its past can say so: clients compare the epoch in `helloAck`, and a
 * different one is the instruction to drop every mirror wholesale. Empty the tables and leave the
 * epoch alone and you get the one state the protocol has no answer for — a server at rev 0 and
 * clients at rev 4,000,000, each convinced the other is behind.
 *
 * AND THE GATEWAYS MUST BE RESTARTED AFTER THIS, which is the step whose omission is silent.
 * `PostgresStorage#init` settles the epoch ONCE per adapter and every later call awaits that same
 * promise, so a running gateway keeps serving the OLD epoch from memory. Until it restarts it will
 * hand a connecting client the dead generation over an empty tree: the client sees a familiar epoch,
 * keeps its mirror, and quietly disagrees with the server about everything.
 *
 * There is a window between the wipe and those restarts. It is not closed here, and pretending
 * otherwise would be worse than naming it: a client connected during that window is in exactly the
 * state described above. Restart both gateways immediately, or do this while nothing is connected.
 */
import { randomInt } from 'node:crypto';
import pg from 'pg';

const url = process.env['RTDB_PG_URL'];
if (!url) {
  console.error('RTDB_PG_URL is required — this script only ever talks to the database you name.');
  process.exit(2);
}
const schema = process.env['RTDB_PG_SCHEMA'] ?? 'public';
const go = process.argv.includes('--yes');

const client = new pg.Client({ connectionString: url });
await client.connect();

try {
  await client.query(`SET search_path = ${schema}`);

  const count = async (t: string): Promise<string> =>
    (await client.query<{ n: string }>(`SELECT count(*)::text AS n FROM ${t}`)).rows[0]?.n ?? '?';

  const { rows: before } = await client.query<{ v: string; epoch: string; pruned_through: string }>(
    'SELECT v, epoch, pruned_through FROM rev_counter WHERE shard = 0',
  );
  const cur = before[0];
  if (!cur) {
    console.error(`no rev_counter row in schema "${schema}" — this is not an initialised shard.`);
    process.exit(1);
  }

  const nodes = await count('nodes');
  const oplog = await count('oplog');

  console.log(`schema          ${schema}`);
  console.log(`nodes           ${nodes} rows      -> 0`);
  console.log(`oplog           ${oplog} rows      -> 0`);
  console.log(`rev             ${cur.v}          -> 0`);
  console.log(`pruned_through  ${cur.pruned_through}          -> 0`);
  console.log(`epoch           ${cur.epoch}      -> a new one`);

  if (!go) {
    console.log('\nNothing was changed. Re-run with --yes to apply.');
    process.exit(0);
  }

  // A new generation must actually be new. The collision is vanishingly unlikely and the loop is
  // cheaper than the failure mode, which is a reset that announces itself as continuity.
  let epoch = randomInt(1, 2 ** 31);
  while (String(epoch) === String(cur.epoch)) epoch = randomInt(1, 2 ** 31);

  // One transaction: a half-applied reset — an empty tree still claiming the old generation — is
  // the exact state this script exists to avoid.
  await client.query('BEGIN');
  await client.query('TRUNCATE nodes, oplog');
  await client.query('UPDATE rev_counter SET v = 0, pruned_through = 0, epoch = $1 WHERE shard = 0', [epoch]);
  await client.query('COMMIT');

  console.log(`\ndone. epoch ${cur.epoch} -> ${epoch}`);
  console.log('\nNOW RESTART BOTH GATEWAYS. They cached the old epoch at boot and will keep serving it:');
  console.log('  aws ssm send-command --instance-ids <gw> --document-name AWS-RunShellScript \\');
  console.log('    --parameters \'commands=["systemctl restart rtdb.service"]\'');
} catch (e) {
  await client.query('ROLLBACK').catch(() => undefined);
  console.error(String(e instanceof Error ? e.message : e));
  process.exit(1);
} finally {
  await client.end();
}
