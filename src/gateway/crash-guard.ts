/**
 * The process-level backstop under `server.ts`'s per-connection catches (WORKLOAD §5.10, §5.11).
 *
 * IMPORTING THIS MODULE IS THE WHOLE POINT. It registers on import and exports nothing, so
 * `main.ts`'s `import './crash-guard.ts'` reads like an unused import and is LOAD-BEARING: delete
 * it in a tidy-up and the gateway silently goes back to dying on any rejection nobody anticipated.
 * That is the same disease as `server.ts`'s `sendPingsAutomatically: false` — a line whose
 * importance is invisible at the point of deletion. It is why `test/unit/unhandled-rejection.test.ts`
 * asserts the import is still present in `main.ts`: a test of the handler alone cannot see the line
 * disappear, which is exactly the false-green that §7 recorded against the previous version.
 *
 * It LOGS AND DOES NOT EXIT, deliberately. Node terminates on an unhandled rejection by default and
 * `deploy/compose.prod.yml`'s `restart: unless-stopped` restarts the container, so the default is a
 * crash LOOP: each restart drops every other connection on this gateway and the NLB moves them onto
 * its twin, under the condition that just killed this one. A handler that logs and then exits — the
 * shape `scripts/console-admin-set.ts` uses, correctly, because an operator CLI SHOULD die on an
 * unexpected rejection — would change the log line and nothing else.
 *
 * SCOPE — it is for STEADY STATE, and it does not reach the boot path. A rejected top-level await
 * in `main.ts` is an ESM module evaluation failure, not an unhandledRejection: Node terminates with
 * a non-zero exit whether or not this handler is installed, so `compose.prod.yml`'s
 * `restart: unless-stopped` still applies to a failed boot. Measured, both directions, in
 * `test/unit/unhandled-rejection.test.ts` — §5.11 Gate B predicted a zombie here (alive, never
 * listening, never restarted) and the prediction was wrong. Boot failure is fatal by design; this
 * guard is for the per-connection surprises that come after it.
 *
 * This is a BACKSTOP, not a fix. Anything reaching it is a rejection no per-path catch anticipated,
 * so it is logged loudly enough to be found and named.
 */
process.on('unhandledRejection', (reason) => {
  process.stderr.write(
    `${JSON.stringify({
      ts: new Date().toISOString(),
      ev: 'unhandled-rejection',
      err: String(reason instanceof Error ? (reason.stack ?? reason.message) : reason),
    })}\n`,
  );
});
