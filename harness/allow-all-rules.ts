/**
 * The rules module the HARNESS runs, and the only one that may be this open.
 *
 * `main.ts` refuses to boot on Postgres without `RTDB_RULES`, because a real deployment that forgot
 * it would let any authenticated client write any path (§5.19). Every gateway the scenarios spawn is
 * a real `main.ts` against a real database, so every one of them meets that guard — and the suites
 * write arbitrary paths as arbitrary subjects, which is the one policy no real deployment wants.
 *
 * This is NOT a bypass, and the distinction is the whole reason it is a file: the guard asks the
 * operator to make a choice, and this file is the harness making it, out loud, where it can be read.
 * It re-exports `allowAll` rather than restating it, so there is one open policy in the codebase and
 * not two that can drift.
 */
export { allowAll as rules } from '../src/pipeline/rules.ts';
