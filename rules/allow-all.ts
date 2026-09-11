/**
 * The rules module PRODUCTION runs, and it authorizes everything.
 *
 * That is a decision, made out loud, where it can be read — which is the whole reason
 * `main.ts` refuses to boot on Postgres without `RTDB_RULES` rather than defaulting to this
 * (§5.19). The guard asks the operator to choose; this file is the choice.
 *
 * WHY IT IS THIS AND NOT `own-subtree.ts`, today:
 *
 *  - **There is no app.** §7's own ruling — a 99.97% CAS failure rate read as a production
 *    incident turned out to be a three-day-old load test, because nobody had asked whether an
 *    app existed. It does not. Confining writes to a subtree confines nothing that exists.
 *  - **The shadow kit would break.** Its subjects are `shadow-<device>` and they write
 *    `UserStatus/<uid>` — a path that is not their own subtree by any reading, so
 *    `own-subtree.ts` refuses the one client the deployment actually has (§7b).
 *  - **The wall that matters is already elsewhere.** §5.20's `outsideOwnDatabase` confines a
 *    token that carries an `ns` claim to that database, and it runs BEFORE rules — so an app
 *    token is confined whatever this file says. What this file governs is the unscoped
 *    subjects: the shadow kit, and the console (which §5.9 governs separately).
 *
 * Moving to `own-subtree.ts` is a decision to make WITH the first real app, when there is a
 * subject shape to confine and someone to break. It is not a deploy-time decision, and making
 * it here would be choosing a policy for an application nobody has written yet.
 *
 * It re-exports `allowAll` rather than restating it, so there is ONE open policy in the
 * codebase and not two that can drift — the same reason `harness/allow-all-rules.ts` exists
 * and does the same thing for the test suites.
 */
export { allowAll as rules } from '../src/pipeline/rules.ts';
