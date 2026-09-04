import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

/**
 * WORKLOAD §5.11 Gate A: `src/gateway/crash-guard.ts`, the process-level backstop — tested by
 * importing THE MODULE ITSELF, out of process.
 *
 * The previous version of this file retyped the handler inline, so what it tested was a pattern and
 * not the production code. §7 recorded that as a false-green shape in its own right: reinstating
 * `process.exit` in `main.ts` left the whole battery green. Every child below now imports the real
 * module, so that mutation goes red here.
 *
 * Out of process because node:test installs its own unhandled-rejection handling at run time: inside
 * this battery Node's production default is not in force, and a rejection cannot be observed doing
 * what it does on the box. Containment — the failing socket closes, other connections keep working —
 * is proved separately in `test/integration/lifecycle.test.ts`.
 */
const GUARD = fileURLToPath(new URL('../../src/gateway/crash-guard.ts', import.meta.url));
const MAIN = fileURLToPath(new URL('../../src/gateway/main.ts', import.meta.url));

const run = (body: string): { status: number | null; stdout: string; stderr: string } => {
  const r = spawnSync(process.execPath, ['--import', 'tsx', '--input-type=module', '-e', body], {
    encoding: 'utf8',
  });
  return { status: r.status, stdout: r.stdout, stderr: r.stderr };
};

/** Reject, then try to do work on a later tick. Reaching the work is what "survived" means. */
const REJECT_THEN_WORK = `
  setTimeout(() => process.stdout.write('ALIVE'), 200);
  Promise.reject(new Error('rds failover killed the query'));
`;

test('§5.11: an unhandled rejection terminates an unguarded process — the premise being contained', () => {
  const bare = run(REJECT_THEN_WORK);
  assert.notEqual(bare.status, 0, 'Node default is terminate; if this ever passes, re-read §5.10');
  assert.equal(bare.stdout, '', 'it never reached the work scheduled after the rejection');
});

test('§5.11: importing crash-guard.ts keeps the process serving, and writes one line', () => {
  const guarded = run(`import ${JSON.stringify(GUARD)};\n${REJECT_THEN_WORK}`);
  assert.equal(guarded.status, 0, 'the real module, not a retyped copy of its shape');
  assert.equal(guarded.stdout, 'ALIVE', 'and it reached the work — which is the whole point');
  assert.match(guarded.stderr, /"ev":"unhandled-rejection"/, 'the incident is written down');
  assert.doesNotMatch(guarded.stderr, /process\.exit/, 'and nothing was swallowed silently');
});

test('§5.11: main.ts imports the guard — the one line no behavioural test can see', () => {
  // The residual the module extraction leaves behind. `import './crash-guard.ts'` binds nothing and
  // reads like dead weight, so a tidy-up deletes it and every behavioural test above stays green:
  // they exercise the module, never main's use of it. A static assertion is ugly and it is the only
  // thing that fails when the line goes. Same disease as `sendPingsAutomatically: false`.
  const main = readFileSync(MAIN, 'utf8');
  assert.match(
    main,
    /^import '\.\/crash-guard\.ts';$/m,
    'main.ts must import crash-guard.ts — without it the gateway dies on any unanticipated rejection',
  );
  assert.doesNotMatch(
    main,
    /process\.on\(\s*['"]unhandledRejection['"]/,
    'and must not register its own handler alongside it',
  );
});

test('§5.11 B0: a rejected TOP-LEVEL AWAIT is still fatal — the guard does not reach boot', () => {
  // The boundary, pinned in both directions, because the two behaviours look contradictory and a
  // future reader may "fix" one into the other.
  //
  // §5.11's Gate B order predicted that the crash-guard would swallow a boot failure and leave a
  // ZOMBIE: a container running, never listening, and never restarted because it never died.
  // Measured, that is not what happens. Node treats a rejected top-level await as an ESM module
  // EVALUATION failure, not an unhandledRejection, so it terminates with a non-zero exit whether or
  // not the guard is installed — and `compose.prod.yml`'s `restart: unless-stopped` still works.
  //
  // The control is the test above: the same module, same guard, a rejection AFTER boot, and the
  // process survives. Same file, same handler, opposite outcomes — which is the boundary itself.
  const boot = run(`
    import ${JSON.stringify(GUARD)};
    setInterval(() => {}, 1000);            // a live handle, so exiting cannot be mere idleness
    setTimeout(() => process.stdout.write('ZOMBIE'), 300);
    const start = async () => { throw new Error('pool exhausted at boot'); };
    await start();                          // exactly main.ts's shape: top-level await on startup
  `);
  assert.notEqual(boot.status, 0, 'boot failure is fatal, so the restart policy still applies');
  assert.equal(boot.stdout, '', 'no zombie: it never reached work scheduled after the failure');
  assert.doesNotMatch(
    boot.stderr,
    /"ev":"unhandled-rejection"/,
    'and the guard never saw it — a top-level await rejection is not an unhandledRejection',
  );
});

test('§5.11: the exiting CLI pattern is NOT a backstop — it only reproduces the default', () => {
  // `scripts/console-admin-set.ts`'s shape: log, then exit. Correct for an operator CLI, which
  // SHOULD die on an unexpected rejection; useless in a server, because Node already terminates by
  // default, so it changes the log line and nothing else. This pins down WHY the pattern is
  // forbidden. Guarding `main.ts` against it is the job of the static assertion above, not this.
  const exiting = run(`
    process.on('unhandledRejection', (e) => { console.error(String(e)); process.exit(2); });
    ${REJECT_THEN_WORK}
  `);
  assert.notEqual(exiting.status, 0, 'still dies');
  assert.equal(exiting.stdout, '', 'still never reaches the work; only the log line differs');
});
