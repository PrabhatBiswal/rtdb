/**
 * A rules module: each user may read and write only under their own id, and nowhere else.
 *
 *   RTDB_RULES=rules/own-subtree.ts node --import tsx src/gateway/main.ts
 *
 * This is an EXAMPLE, and it is deliberately the smallest useful one. Copy it, change the policy,
 * point RTDB_RULES at yours. What it demonstrates is the shape: a `Rules` function is called with
 * the subject, the operation and the path, once per write and once per `listen` — never per delta —
 * and returning false refuses that one operation.
 *
 * WITHOUT a rules module the gateway runs `allowAll`, and then authentication is the only thing
 * standing between a token and the whole tree: any client that can connect can write any path,
 * including top-level ones that nobody declared. Namespaces are not declared anywhere — a namespace
 * is the first segment of a path and it comes into existence on the first write under it — so
 * "unknown namespace" is not a thing the server can refuse on its own. This file is where it becomes
 * one.
 *
 * THE CONSOLE IS LET THROUGH ON PURPOSE, and getting this wrong is the trap. Console sessions carry
 * `console-…` subjects, which match no user's subtree, so a naive own-subtree rule locks the console
 * out of the tree it exists to administer. What may write from a console is already decided by
 * §5.9's invariant in `src/pipeline/rules.ts`, which the write pipeline consults BEFORE this function
 * and which no rules module can vote down. Deciding it again here can only ever be wrong in one
 * direction.
 */
import type { Rules } from '../src/pipeline/rules.ts';

/** Subjects the console mints. §5.9's invariant already governs what they may write. */
const CONSOLE_PREFIX = 'console-';

export const rules: Rules = ({ userId, path }) => {
  if (userId.startsWith(CONSOLE_PREFIX)) return true;
  // Exactly the user's own node, or something under it. `startsWith(userId)` alone would be a bug:
  // it lets `u_1` reach `u_12`'s subtree, and that reads correct until the day two ids share a
  // prefix — which is the first day it matters.
  return path === userId || path.startsWith(`${userId}/`);
};

export default rules;
