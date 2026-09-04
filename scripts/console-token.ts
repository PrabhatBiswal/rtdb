/**
 * Mint a SHORT-LIVED token for console/rtdb-console.html.
 *
 *   node --import tsx scripts/console-token.ts --name asha [--role owner] [--hours 1]
 *                                              [--profile rtdb-deploy]
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
 * The secret is read from SSM at run time and never written anywhere: not to a file, not to the
 * shell history, not into the console page. ONLY the token is printed, so `| pbcopy` is safe.
 */
import { execFileSync } from 'node:child_process';
import { signDevToken } from '../src/gateway/auth.ts';

const flag = (name: string): string | undefined => {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? undefined : process.argv[i + 1];
};

const name = flag('name');
if (!name || !/^[a-z0-9][a-z0-9_-]*$/i.test(name)) {
  console.error('usage: console-token.ts --name <who> [--hours 1] [--profile rtdb-deploy]');
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

const profile = flag('profile') ?? 'rtdb-deploy';
const region = process.env['AWS_REGION'] ?? 'ap-south-1';

let secret: string;
try {
  secret = execFileSync(
    'aws',
    ['ssm', 'get-parameter', '--name', '/rtdb/prod/jwt_secret', '--with-decryption',
     '--query', 'Parameter.Value', '--output', 'text', '--profile', profile, '--region', region],
    { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
  ).trim();
} catch (e) {
  // Fail loud and specific: a missing profile and a denied parameter are different problems.
  console.error(`could not read /rtdb/prod/jwt_secret with profile "${profile}" in ${region}:`);
  console.error(String((e as { stderr?: Buffer }).stderr ?? e).trim().split('\n').slice(-2).join('\n'));
  process.exit(1);
}
if (!secret) {
  console.error('SSM returned an empty secret — refusing to mint a token nobody can verify');
  process.exit(1);
}

const exp = Math.floor(Date.now() / 1000) + Math.round(hours * 3600);
// stderr, so `console-token.ts --name x | pbcopy` copies the token and nothing else.
console.error(`console token for "console-${name}" as ${role}, expires ${new Date(exp * 1000).toISOString()} (${hours}h)`);
process.stdout.write(`${signDevToken({ sub: `console-${name}`, exp, role }, secret)}\n`);
