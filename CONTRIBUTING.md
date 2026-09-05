# Contributing

Thanks for looking. A few things are worth knowing before you spend an evening on this.

## This repository is published as a snapshot

The working tree lives in a private repository and this one is re-derived from it, in full, on every
release. Two consequences:

- **Pull requests are applied upstream, not merged here.** Your PR will usually close without a merge
  commit and your change will arrive in the next export. Authorship is preserved in the commit; the
  message may be rewritten to match the surrounding style.
- **A change made only here is lost at the next export.** That is fine for a PR — it is a patch, and
  patches get applied — but do not expect a direct push to survive.

Small, self-contained patches are much easier to carry across than large refactors. If you are
planning something big, open an issue first and we will find out quickly whether it fits.

## Running everything

Node 22+. **Every command runs from the repository root** — the sources execute through `tsx`, which
is a local dependency, so running one from elsewhere reports `Cannot find package 'tsx'`.

```bash
npm ci
npm run check      # typecheck + 221 unit and integration tests. No infrastructure needed.
npm run chaos      # 11 scenarios, including SIGKILL on a gateway under live traffic.
npm run test:pg    # needs a Postgres; tests create and drop their own databases.
npm run test:bus   # needs a Redis binary on PATH — the suites kill the Redis they spawned.
cd sdk-kotlin && ./gradlew check    # spawns the real Node gateway; no mock servers.
```

CI runs all of it, plus a build-boot-scrape-SIGTERM cycle on the Docker image. A PR that has not run
`npm run check` and `npm run chaos` locally will find out the slow way.

## The wire protocol is frozen

`PROTOCOL.md` is v1.5 and normative. Both SDKs, the console and the test harness are all clients of
it, so a frame change is never a local change. If you need one: add a §12 changelog entry, bump the
version, and say in the PR what an old client does when it meets a new server. Unknown frame types
and unknown fields must stay ignorable — that rule is what lets extensions ship without breaking
anyone.

`PROTOCOL.md` §11 lists extensions that are designed but not implemented — queries, `push()`, server
time, `incr`, one-shot `get`. Implementations of those are welcome and should follow the design that
is written there. A redesign of one is a discussion first.

## Style

Commit messages here explain **why**, not what — the diff already says what. Comments earn their
place by recording a decision or a hazard that the code cannot show. Tests are teeth: a test that
cannot fail is not a test, and a scenario that has caught a real bug is worth more than three that
assert the happy path again.

## Settled, and not worth reopening

- The `com.hobostays` Maven coordinates. Apache-2.0 lets you fork and publish under your own; the
  README says how.
- §4's CAS lock ordering. It closes a dedup race on purpose and is normative.
