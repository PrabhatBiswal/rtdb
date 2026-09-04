/**
 * Set a console user's password. Run by the OPERATOR, locally.
 *
 *   node --import tsx scripts/console-admin-set.ts --email you@example.com [--role owner]
 *                                                  [--profile rtdb-deploy]
 *
 * This writes into the SAME multi-user store the console's Users panel writes, `/rtdb/console/users`
 * — read-modify-write, every other user and every role left exactly as it was. It is the recovery
 * path: the panel needs an owner who can already sign in, and this is how that owner gets a password
 * when nobody can. An existing user keeps their role unless --role says otherwise; a new one needs
 * --role stated, except when the store is empty and the first user can only be the owner.
 *
 * The password is typed at a prompt with ECHO OFF and exists only in this process's memory. It is
 * never an argument (argv is visible in `ps`), never an env var, never written to a file, never
 * logged, and never sent anywhere: what reaches SSM is a scrypt hash and a random salt.
 *
 * This script is the ONLY path by which a console password is set. If a password has ever been
 * typed somewhere it could be read — a chat, a ticket, a shell history — it is burned; choose a
 * different one here.
 */
import { execFileSync } from 'node:child_process';
import { createInterface } from 'node:readline';
import { randomBytes, scryptSync, timingSafeEqual } from 'node:crypto';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const flag = (n: string): string | undefined => {
  const i = process.argv.indexOf(`--${n}`);
  return i === -1 ? undefined : process.argv[i + 1];
};

const email = flag('email');
if (!email || !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) {
  console.error('usage: console-admin-set.ts --email <address> [--role owner|editor|viewer] [--profile rtdb-deploy]');
  process.exit(2);
}
const profile = flag('profile') ?? 'rtdb-deploy';
// Overridable so this can be exercised against a throwaway parameter. It defaults to the real one,
// and the default is what anybody setting a password actually types.
const param = flag('param') ?? '/rtdb/console/users';
// The pre-roles single-admin record. Read only as a fallback, never written: it is what the server
// falls back to until the user store exists, and it stays as the rollback path.
const legacyParam = flag('legacy-param') ?? '/rtdb/console/admin';
const region = process.env['AWS_REGION'] ?? 'ap-south-1';

const ROLES = ['owner', 'editor', 'viewer'] as const;
type Role = (typeof ROLES)[number];
const wantedRole = flag('role');
if (wantedRole !== undefined && !ROLES.includes(wantedRole as Role)) {
  console.error(`--role must be one of ${ROLES.join(', ')}; got "${wantedRole}"`);
  process.exit(2);
}

type Record_ = { salt: string; hash: string; params: typeof PARAMS; role?: Role };

/**
 * Self-hosting without AWS: with CONSOLE_STORE_DIR set, the store is a file in that directory,
 * exactly as the auth server reads it under the same variable. Unset, every path here is what it
 * was — SSM read, SSM write. This is the only way to create the FIRST owner on a local install,
 * since the console's own Users panel needs an owner who can already sign in.
 */
const STORE_DIR = process.env['CONSOLE_STORE_DIR'];
const localFile = (name: string): string =>
  join(STORE_DIR as string, name.replace(/^\//, '').replace(/\//g, '_') + '.json');

const ssmGet = (name: string): string => {
  if (STORE_DIR) {
    // The caller distinguishes an absent store from a failed read and falls back only on the
    // former, so a missing file has to look like SSM's absence rather than like a broken read.
    try {
      return readFileSync(localFile(name), 'utf8').trim();
    } catch (e: unknown) {
      if ((e as { code?: string }).code !== 'ENOENT') throw e;
      throw new Error(`ParameterNotFound: ${name}`);
    }
  }
  return execFileSync('aws', ['ssm', 'get-parameter', '--name', name, '--with-decryption',
    '--query', 'Parameter.Value', '--output', 'text', '--profile', profile, '--region', region],
    { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
};

/**
 * The store as it stands. A missing user store falls back to the legacy single-admin record (that
 * user is the owner — they were the only user there had ever been); a missing legacy record too
 * means there is nothing yet, and this run creates the store.
 *
 * Only ParameterNotFound is treated as "absent". A denial or a network failure must not be read as
 * an empty store — that would quietly overwrite every other user with one record.
 */
function loadStore(): Record<string, Record_> {
  try {
    const parsed = JSON.parse(ssmGet(param)) as Record<string, Record_>;
    const out: Record<string, Record_> = {};
    for (const [e, rec] of Object.entries(parsed)) out[e.toLowerCase()] = rec;
    return out;
  } catch (e) {
    const why = String((e as { stderr?: Buffer }).stderr ?? e);
    if (!/ParameterNotFound/i.test(why)) {
      console.error(`could not read ${param} with profile "${profile}" in ${region}:`);
      console.error(why.trim().split('\n').slice(-2).join('\n'));
      process.exit(1);
    }
  }
  try {
    const admin = JSON.parse(ssmGet(legacyParam)) as { email: string } & Record_;
    return { [String(admin.email).toLowerCase()]: { salt: admin.salt, hash: admin.hash, params: admin.params, role: 'owner' } };
  } catch (e) {
    if (!/ParameterNotFound/i.test(String((e as { stderr?: Buffer }).stderr ?? e))) {
      console.error(`could not read ${legacyParam}:`);
      process.exit(1);
    }
    return {};
  }
}

/**
 * scrypt at a deliberately slow setting: this guards ONE password against an offline attacker.
 * `maxmem` is explicit because Node caps scrypt at 32 MiB by default and N=2^15,r=8 needs exactly
 * 128*N*r = 32 MiB — it fails with "memory limit exceeded" without this, and the verifier must be
 * given the same ceiling or a hash written here cannot be checked there.
 */
const MAXMEM = 64 * 1024 * 1024;
const PARAMS = { N: 2 ** 15, r: 8, p: 1, keylen: 64 } as const;

/** Read a line with the terminal's echo turned off, restoring it whatever happens. */
function askHidden(prompt: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const { stdin, stdout } = process;
    if (!stdin.isTTY) {
      reject(new Error('refusing to read a password from a pipe — run this in a terminal'));
      return;
    }
    stdout.write(prompt);
    const rl = createInterface({ input: stdin, output: stdout, terminal: true });
    // readline echoes; silence the output stream for the duration instead of echoing asterisks,
    // so the password never reaches the terminal buffer or a scrollback capture.
    const mute = (chunk: unknown, enc: unknown, cb?: () => void): boolean => {
      if (typeof cb === 'function') cb();
      return true;
    };
    const realWrite = (stdout as unknown as { write: unknown }).write;
    (stdout as unknown as { write: unknown }).write = mute;
    rl.question('', (answer) => {
      (stdout as unknown as { write: unknown }).write = realWrite;
      stdout.write('\n');
      rl.close();
      resolve(answer);
    });
    rl.on('SIGINT', () => {
      (stdout as unknown as { write: unknown }).write = realWrite;
      stdout.write('\naborted; nothing was written\n');
      process.exit(130);
    });
  });
}

// A rejected prompt is an operator error, not a crash: say what to do and exit cleanly.
process.on('unhandledRejection', (e) => {
  console.error(String(e instanceof Error ? e.message : e));
  process.exit(2);
});

const pw = await askHidden(`password for ${email}: `).catch((e: Error) => {
  console.error(e.message);
  process.exit(2);
});
if (pw.length < 12) {
  console.error(`refusing: ${pw.length} characters. This is the only credential on a page that can `
    + 'read the whole shard; use at least 12.');
  process.exit(1);
}
const again = await askHidden('again: ').catch(() => process.exit(2));
// Constant-time even here: a length-leaking compare on a password is a bad habit to write down.
const a = Buffer.from(pw), b = Buffer.from(again);
if (a.length !== b.length || !timingSafeEqual(a, b)) {
  console.error('refusing: the two entries differ; nothing was written');
  process.exit(1);
}

const store = loadStore();
const key = email.toLowerCase();
const existing = store[key];
// An existing user keeps their role; --role overrides. A new user must have one stated, unless this
// run is creating the store, where the first user can only be the owner.
const role = (wantedRole as Role | undefined) ?? existing?.role
  ?? (Object.keys(store).length === 0 ? 'owner' : undefined);
if (!role) {
  console.error(`${email} is not in ${param} yet — say what they are with --role ${ROLES.join('|')}.`);
  console.error(`existing users: ${Object.keys(store).join(', ')}`);
  process.exit(2);
}

const salt = randomBytes(32);
const hash = scryptSync(pw, salt, PARAMS.keylen, { N: PARAMS.N, r: PARAMS.r, p: PARAMS.p, maxmem: MAXMEM });
store[key] = {
  salt: salt.toString('base64'),
  hash: hash.toString('base64'),
  params: PARAMS,
  role,
};
if (!Object.values(store).some((u) => (u.role ?? 'owner') === 'owner')) {
  console.error('refusing: that would leave the console with no owner; nothing was written');
  process.exit(1);
}

// file:// rather than an argument: argv is readable in `ps`, and this value now carries EVERY
// user's hash, not one. 0600, and removed whatever happens.
const dir = mkdtempSync(join(tmpdir(), 'rtdb-users-'));
const file = join(dir, 'users.json');
try {
  if (STORE_DIR) {
    // 0700 on the directory and 0600 on the file: this holds every user's salt and password hash.
    // SSM never needed a directory to exist, so nothing here used to create one — and the first
    // person to point CONSOLE_STORE_DIR at a path that did not exist got ENOENT after typing their
    // password twice.
    mkdirSync(STORE_DIR, { recursive: true, mode: 0o700 });
    writeFileSync(localFile(param), JSON.stringify(store), { mode: 0o600 });
  } else {
    writeFileSync(file, JSON.stringify(store), { mode: 0o600 });
    execFileSync('aws', ['ssm', 'put-parameter', '--name', param, '--type', 'SecureString',
      '--overwrite', '--value', `file://${file}`, '--profile', profile, '--region', region],
      { stdio: ['ignore', 'ignore', 'pipe'] });
  }
} catch (e) {
  // The AWS wording is wrong for the local store: naming a profile and a region for a failed file
  // write sends the reader to check credentials for something credentials had no part in.
  console.error(STORE_DIR
    ? `could not write ${param} to the local store at ${STORE_DIR}:`
    : `could not write ${param} with profile "${profile}" in ${region}:`);
  console.error(String((e as { stderr?: Buffer }).stderr ?? e).trim().split('\n').slice(-2).join('\n'));
  process.exit(1);
} finally {
  rmSync(dir, { recursive: true, force: true });
}

console.log(`set. ${param} now holds a scrypt hash for ${email} as ${role} `
  + `(N=${PARAMS.N}, r=${PARAMS.r}, p=${PARAMS.p}); ${Object.keys(store).length} user(s) in the store.`);
console.log('The password itself was never written, logged, or transmitted.');
