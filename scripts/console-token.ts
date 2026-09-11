/**
 * Mint a SHORT-LIVED token for console/rtdb-console.html.
 *
 *   RTDB_DEV_SECRET=... node --import tsx scripts/console-token.ts --name asha   # local gateway
 *   node --import tsx scripts/console-token.ts --name asha [--role owner] [--hours 1]
 *                                              [--database chat] [--profile rtdb-deploy]
 *
 * The point is the habit, not the cryptography: without this, an operator opening the console
 * reaches for whatever token is nearest, and the nearest one is long-lived and production. This
 * mints a fresh one that expires in an hour and names who it belongs to, so a token seen in a log
 * or a screen-share is attributable and already close to worthless.
 *
 * `sub` is `console-<name>` — a distinct subject, so §10's kick can revoke every console session
 * without touching the app's users. `role` is the claim the auth-server's /stats, /topnodes and
 * /users check; a token minted without one is refused by all three, which is exactly what keeps a
 * device's shadow token from reading the fleet.
 *
 * WHAT THIS MINTS IS READ-ONLY ON THE DATA PATH, AND `--role owner` DOES NOT CHANGE THAT.
 * `consoleWriteDenied` (`src/pipeline/rules.ts`) demands TWO things of a console write, not one: a
 * write role AND the subject prefix `console-rw-`, which is the deliberate unlock `/login` mints for
 * editor and owner (`auth-server.mjs`: `console-${WRITE_ROLES.has(role) ? 'rw-' : ''}…`). This
 * script only ever builds `console-<name>`, so its token reads the whole tree — sidebar, panel,
 * /usage, /topnodes — and every `put` comes back `RULES`, however owner-ish the role claim looks.
 * That is deliberate, not a gap: a token minted from a shell for a look around should not be able to
 * edit production, and the one credential that can write is the one a human logged in for. The
 * witness is `test/integration/console-write.test.ts` — "the role alone is not enough: editor on a
 * plain console- subject is refused"; it names editor, and owner takes the same branch because the
 * missing `rw-` prefix already decides it, whatever the role.
 *
 * So: for reading, use it. To WRITE as the console, log in to the console. If you genuinely need a
 * writing token from here, `--name rw-<who>` produces `console-rw-<who>` and, with an editor or
 * owner role, passes the gate — spelled out because it is the kind of thing that gets rediscovered
 * as a bug at 3am, and because it means the `--name` flag is load-bearing beyond attribution.
 *
 * The secret comes from RTDB_DEV_SECRET when it is set, and from SSM otherwise — so this works
 * against a local gateway with no AWS at all, and against the deployment when you have the profile.
 * Either way it is never written anywhere: not to a file, not to the shell history, not into the
 * console page. ONLY the token is printed, so `| pbcopy` is safe.
 */
import { execFileSync } from 'node:child_process';
import { signDevToken } from '../src/gateway/auth.ts';
import { DEFAULT_LIMITS } from '../src/protocol/limits.ts';
import { validateDatabaseName } from '../src/protocol/path.ts';

const flag = (name: string): string | undefined => {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? undefined : process.argv[i + 1];
};

const name = flag('name');
if (!name || !/^[a-z0-9][a-z0-9_-]*$/i.test(name)) {
  console.error('usage: console-token.ts --name <who> [--hours 1] [--database chat] [--profile rtdb-deploy]');
  console.error('  --name identifies the human, so a leaked token is attributable and kickable.');
  process.exit(2);
}

const hours = Number(flag('hours') ?? 1);
if (!Number.isFinite(hours) || hours <= 0 || hours > 24) {
  console.error(`--hours must be in (0, 24]; got ${flag('hours')}`);
  process.exit(2);
}

const role = flag('role') ?? 'owner';
if (!['owner', 'editor', 'viewer'].includes(role)) {
  console.error(`--role must be owner, editor or viewer; got "${role}"`);
  process.exit(2);
}

/**
 * §5.22 Gate F: which DATABASE this token is for. Omitted is the correct spelling of "the default
 * tenant" — `rules.ts` reads an absent `ns` as "names no database", and an empty one is refused
 * outright by `auth.ts`, so this is a claim that is either present and real or not there at all.
 *
 * The served console mints these itself through `POST /wire-token`. This flag is the file:// path
 * and RUNBOOK §7c recovery, where there is no auth-server to ask.
 */
const database = flag('database');
const badDb = database === undefined ? null : validateDatabaseName(database, DEFAULT_LIMITS);
if (badDb) {
  console.error(`--database ${database}: ${badDb}`);
  process.exit(2);
}

const profile = flag('profile') ?? 'rtdb-deploy';
const region = process.env['AWS_REGION'] ?? 'ap-south-1';

/**
 * Self-hosting without AWS: RTDB_DEV_SECRET is the same variable the gateway is given, so a token
 * minted here is one that gateway can verify. The console's auth server and console-admin-set.ts
 * take the same fallback; this file was missed when they got it, and the README meanwhile offered
 * this script as the local helper — so on any machine without the deploy profile the documented
 * way to get a token could not work at all, and on a machine WITH it the result was worse: a token
 * signed with the PRODUCTION secret, which a local gateway rejects with close 4401.
 *
 * The env var wins when it is set. That ordering matters: reaching for SSM first would make a
 * developer with credentials silently mint production-signed tokens for a local server.
 */
let secret: string;
const devSecret = process.env['RTDB_DEV_SECRET'];
if (devSecret) {
  secret = devSecret;
} else try {
  secret = execFileSync(
    'aws',
    ['ssm', 'get-parameter', '--name', '/rtdb/prod/jwt_secret', '--with-decryption',
     '--query', 'Parameter.Value', '--output', 'text', '--profile', profile, '--region', region],
    { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
  ).trim();
} catch (e) {
  // Fail loud and specific: a missing profile and a denied parameter are different problems.
  console.error(`could not read /rtdb/prod/jwt_secret with profile "${profile}" in ${region} (set RTDB_DEV_SECRET to mint a token for a local gateway instead):`);
  console.error(String((e as { stderr?: Buffer }).stderr ?? e).trim().split('\n').slice(-2).join('\n'));
  process.exit(1);
}
if (!secret) {
  console.error('SSM returned an empty secret — refusing to mint a token nobody can verify');
  process.exit(1);
}

const exp = Math.floor(Date.now() / 1000) + Math.round(hours * 3600);
// stderr, so `console-token.ts --name x | pbcopy` copies the token and nothing else.
console.error(
  `console token for "console-${name}" as ${role} on ${database ?? 'the default database'}, ` +
    `expires ${new Date(exp * 1000).toISOString()} (${hours}h)`,
);
process.stdout.write(
  `${signDevToken({ sub: `console-${name}`, exp, role, ...(database ? { ns: database } : {}) }, secret)}\n`,
);
