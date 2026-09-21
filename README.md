# rtdb

A self-hostable realtime database with a Firebase-Realtime-Database-shaped client API.

You run the server on your own infrastructure, point the SDK at your own URL, and get the parts
that matter: a live tree, value and child listeners, offline writes that replay on reconnect,
compare-and-swap for contended values, and horizontal fanout across more than one gateway.

- **Server** — Node 22, WebSocket gateway, Postgres for durability, Redis Streams for cross-gateway
  fanout. Postgres and Redis are both optional: the gateway runs standalone with in-memory storage.
- **Clients** — a Kotlin core SDK and an Android SDK. The client API is deliberately close to
  Firebase's (`ref`, `setValue`, `addValueEventListener`, `DataSnapshot`, child events), so porting
  an app is mostly mechanical.
- **Not tied to this repo's deployment.** The SDK talks to any `wss://` URL that speaks the
  protocol. Nothing in it is hardcoded to a particular backend.

### What Firebase has that this does not

"Mostly mechanical" is a claim about call sites, not about coverage. These are the gaps a port hits,
as of protocol v1.6:

| Firebase | Here |
|---|---|
| Security Rules | **Nothing ships.** `allowAll` is the only implementation — see [Authentication](#authentication) before you deploy. |
| Queries — `orderByChild`, `limitToLast`, `startAt`, `equalTo` | Absent. §11 E2 designs `orderByKey` with `limitToFirst`/`limitToLast`; `orderByChild` needs server-side indexes and is explicitly out of scope. |
| `push()` | Absent — generate keys yourself. §11 E3 specifies Firebase's exact push-id algorithm, so that keys interleave correctly if you ever dual-write against a real Firebase. |
| `ServerValue.TIMESTAMP` | Absent. §11 E4 puts server time in `helloAck` and resolves `{".sv":"timestamp"}` at commit. |
| `runTransaction()` | Not wrapped. Compare-and-swap is the shipped answer: read the rev you saw, write against it, retry with what won. |
| `onDisconnect()` / presence | Absent, and not designed. No protocol support at all. |
| `keepSynced()`, disk persistence | Absent. The mirror is memory-only; a cold start re-fetches. Offline *writes* do survive a disconnect — offline *reads* do not survive a process restart. |
| One-shot `get()` | Absent — subscribe and take the first snapshot. §11 E1. |
| REST API | None. WebSocket only, so no `curl`-able reads and no webhook-shaped integrations. |
| Priorities, `onChildMoved`, `previousChildName` | Deliberately not implemented. Ordering arrives with §11 E2's windowed queries; until there is a server-side order, a "moved" event has nothing to be moved within. |
| iOS / Swift SDK | Absent. Kotlin (plain JVM) and Android today; the protocol is public, so a third SDK is work, not a design question. |
| Multi-region | Absent. One shard, one region. Nothing in the protocol forbids more; nothing implements it. |
| Google's capacity | Today this runs on ONE gateway instance. That is a deployment fact rather than a design limit — §8 sizes each gateway for 100% of the shard so a second is a Terraform variable — but it is the honest shape of what has been measured. |

`.info/connected` does work, and so do `updateChildren` across several keys, child events, and the
offline write queue.

The rows carrying an E-number are designed in [`PROTOCOL.md`](PROTOCOL.md) §11 — design-ready,
scheduled separately, not implemented. Design-ready is not a delivery date. Everything else in that
table is absent by decision.

### What works differently, and why

Same call sites, different machinery underneath. A comparer should look here rather than at the API
surface, because this is where behaviour actually diverges.

- **Reconnect resumes from a revision, not from a re-listen.** A client stores the last rev it saw
  per subscription and sends it with `listen`; the server replies with the deltas since that rev, or
  a fresh snapshot if it cannot serve them ([`PROTOCOL.md`](PROTOCOL.md) §3, §6). Firebase re-syncs
  by comparing hashes and re-sending what differs. *Measured on three handsets against a live
  deployment: one phone offline through three writes it never saw, radio flapping on the way back
  up — the reconnect was served as two catch-ups and zero snapshots, and all three missed children
  arrived exactly once, in key order. Witness: the gateway's own `listens{result}` counters
  (`catchup` +2, `snapshot` +0) plus the phone's glass.*
- **Ordering is a property of the write path, not of the client.** Every write for a shard passes
  one dispatcher and one Redis stream, so frames leave in commit order and a client never
  gap-detects or reorders ([`PROTOCOL.md`](PROTOCOL.md) §8). Gaps in a subscription's rev sequence
  are normal and are not an error.
- **The client mirror defends itself with per-leaf revisions and tombstones.** Every leaf carries the
  rev that wrote it; a delta older than the leaf it touches is dropped, and a delete leaves a
  rev-stamped tombstone so a late frame cannot resurrect it ([`PROTOCOL.md`](PROTOCOL.md) §7). It is
  defence in depth — the dispatcher above already guarantees order. *Measured over a 20-minute soak:
  574,000 child events, 111 forced reconnects, zero duplicate states delivered to a reading client
  and zero removes for a child never added.*
- **Quotas are per database, and the console says where the number came from.** Each database has an
  acquisition-rate budget, and the UI distinguishes a shard default from an explicit override
  instead of showing one blended figure — which matters, because the two fail differently.
  *Measured: see the quota row under [Measured](#measured) — a database can refuse writes while its
  average rate sits at a third of its limit.*
- **The usage meters are bill-shaped, and they publish their own caveats.** Connections include
  console sessions; storage is a high-water mark, not an average; downloads are counted before TLS
  framing; load is utilisation (ρ), not a queue depth. Firebase's equivalents are averaged and their
  definitions are not stated. A meter you cannot reconcile with an invoice is decoration.
- **Cost is a line you can read rather than a bill that arrives.** The deployment this was measured
  on ran at **$5.09/day**, identical to the cent every day across a seven-day window
  (2026-09-03 → 09-09, read from the billing API on 09-10) — about **$153/month**, against a
  pre-build estimate of $181/month. For scale, **this project's own** Firebase bill — the one it
  exists to replace, not a customer's — ran at **~$1,700/month**, essentially all of it downloads:
  **1.69 TB/month against 438 MB of stored data**, so every stored byte left the database roughly
  3,800 times a month. Read off the provider's own usage console for the **August 2026 billing
  period (read 2026-08-31)**; it is the number in this project's mission statement, and it is what
  "fat listen shapes" costs when you pay per byte delivered.

### What works the same

Stated plainly, because a migration guide that only lists differences is not honest about the ones
that matter:

- **One changed child sends one delta frame.** Not a re-snapshot of the subtree — the thing most
  people actually mean by "realtime database". *Measured phone-to-phone on a live deployment: a
  single new child produced three delta frames to three subscribers and zero snapshots, in both
  directions.*
- **`child_added` / `child_changed` / `child_removed`,** with a late listener replayed the existing
  children exactly as Firebase does.
- **Optimistic local write.** Your write shows on your own client immediately and settles later,
  including the consequence Firebase also has and rarely documents: on a key two clients are writing
  at once, the writer sees its own value, then the other client's, then its own again. *Measured at
  4.1% of writes issued, on writer clients only; clients that only read saw none of it.* Rendering a
  list straight from a child-event stream on a contended key will flicker, here and there alike.

---

## Measured

**2026-09-21, one `m7g.medium` gateway (1 vCPU, no burst credits) against `db.t4g.medium` Postgres,
one region, one shard.** Every number below carries its shape and its witness. Where a measurement
came from a laptop rather than that deployment, the heading says so — the two are not comparable and
mixing them would be the easiest lie in this file.

### On the deployed gateway

| what | shape | number | witness |
|---|---|---|---|
| **Load scales linearly THROUGH 800 w/s** — a waypoint, not a limit; see the two rows below | 1,000 → 2,000 → 3,000 connections, 200 → 400 → 800 writes/s offered, `hot 0`, 150 s per step, two databases | Commit rate equalled offered rate at every step, **zero write errors**. Gateway CPU **0.145 → 0.280 → 0.373 cores**; connections 1,006 → 2,007 → 3,008; pool waiters 0 throughout | Two client tails per step, plus a server-side window anchored at the step (not at read time) |
| **The tail grows faster than the load** | as above | Client ack p99 **91 ms → 527 ms → 12 s** while p50 stayed at **19 → 20 → 28 ms** | Same. The 12 s is a step-start transient — 3,000 connections subscribing at once while the first writes land — not steady-state saturation; the p50 is the steady state |
| **A quota clips burst shape, not average rate** | 509 connections on one database, ~100 writes/s, `hot 0.1`, ~3 min | **413 writes refused** for rate while the measured average acquisition rate was **23.4/s against a limit of 64/s** | Client tails (413 counted) and an anchored server window (≈444 by extrapolation) |
| **The write path did not reach a ceiling** | 11,006 → 17,006 connections, `hot 0`, two databases, writes offered as fast as the rig could push | **4,010 writes/s at 0.30–0.34 of a core.** Lock acquisitions ~10/s — group commit folding ~400 writes into one — event-loop lag 1.4 ms, pool waiters 0 | Live reads through the ops Prometheus at two points in the rung. **The RIG pegged first: a load client sat at 99% CPU.** So 4,010 w/s is a floor under the gateway's write path, not its ceiling |
| **Cross-database isolation held all the way up** | 1,000 → 17,000 connections and the full write load on two databases; three handsets subscribed to a third | Phone-to-phone arrival **2–5 s at every rung from 1,000 to 17,000 connections** — it did not degrade as the rig climbed, it did not move. The worst arrival of the night, **7 s**, was the one rung whose load was on the phones' OWN database | Three handsets, ±2 s render poll. Two rungs (2,000 and 3,000 connections) have **no trustworthy arrival number and are recorded as holes** — the harness watched a pane that new children scrolled out of — not as failures |
| **The break, when it came, was MEMORY** | ~17,000 connections being torn DOWN | `JavaScript heap out of memory` at **~1.9 GB**, V8's default cap on a 4 GiB box, with **no heap flag set anywhere**. Measured cost: **0.103 MB of heap per connection** over 91 samples — 1,006 connections = 33 MB, 17,006 = 1,860 MB, and the cap is 1,900 | Gateway logs and a regression over the ramp's own samples. **It died during TEARDOWN, about nine seconds after the rig's connections reached zero — not while serving them**, which is a different claim and the weaker one |
| **Recovery was unaided, and bounded by the restart** | the above | Container back in **~5 minutes**; the three handsets reconnected **by themselves 8–29 s after it returned**, having spent the outage in correct widening backoff. **Trees intact** — nothing lost across an OOM kill. The rig's own 7,800 abandoned clients came back as **4,261 catch-ups to 2,972 snapshots** | Each phone's own log, and the gateway's counters for the reconnect storm |
| **Cross-database isolation, at the glass** | 1,000 connections and ~200 writes/s on two databases; three Android handsets subscribed to a third | Arrival on a second phone **~2 s — indistinguishable from the same test with no load at all**, no client dropped to `WAITING` | Three real handsets, arrival read off rendered pixels on a fixed 2 s poll, so **±2 s** and "~2 s" means *at or below the poll floor* |
| **Reconnect catch-up across a radio flap** | one handset offline through three writes, its radio flapping on the way back | **catch-up ×2, snapshot ×0**; all three children arrived exactly once, in order | Gateway `listens{result}` counters and the phone's screen |
| **One-child delta, both directions** | two handsets, one new child each way | **+3 delta frames to three subscribers, +0 snapshots** | Gateway counters, plus both screens rendering byte-identical values |
| **Backgrounding is the handset's decision, not the protocol's** | three handsets, three vendors, app backgrounded ~65 s | **Two of three dropped** the socket 8–10 s after backgrounding; one never dropped. Recovery on return: **~5 s** and **~15 s**, clean, no duplicate events | The decisive one: on both handsets that dropped, *the app's own 5 s log line stopped at the same instant the socket died* — the process was frozen by vendor power management, which is not a protocol event. Going to the background never touches the socket in SDK code |

**What these do not show.** **No WRITE ceiling was found** — the rig ran out of CPU before the
gateway did, so 4,010 w/s is a number this hardware beat, not a number it could not pass. **The
fanout ceiling was not measured at all:** the run that was meant to find it put its fan-out load on
clients that were already saturated, so the delivery rate it produced measures the receivers, not
the server. An earlier figure of 575 w/s per gateway, if you have seen it, was a DELIVERY ceiling at
a different write shape and was never a write ceiling. Server-side ack p99 on this build tops out at
300 s and reports exactly that when it saturates, so a pegged figure means *at least* 300 s. The
handset results cover three devices from three vendors and say nothing about a fourth. And the
memory ceiling above is one measurement of one break: 0.103 MB per connection is this workload's
number — subscriptions, tree shape and payload size all move it.

### On a laptop, not on that gateway

| what | shape | number |
|---|---|---|
| **20-minute SDK soak** | 50 clients in one JVM, each with a value listener and a child-event stream; 5 of them writing; **three sockets killed from outside every 30 s**; in-memory storage | **11,477 writes, 111 forced reconnects.** Every one of the 50 client mirrors equalled the server at all ten checkpoints and at the end. **Zero** duplicate child states on reading clients, **zero** removes-without-add across 574,000 child events. Client mirror bounded by the data, not by the run — tombstones flat from minute 2 to minute 20 |

Run it yourself: `cd sdk-kotlin && ./gradlew soak`. It is excluded from the normal battery because it
takes twenty minutes.

---

## The console

![The RTDB console watching a path: a live tree of three room records with typed values, showing the
connection state, the current revision and the number of open subscriptions](console/screenshot.png)

Sign in, watch a path, edit the tree, manage users across three roles. Expanding a node subscribes
to it and collapsing unsubscribes, so the console is bound by the same `SNAPSHOT_MAX` limit as any
other client — the root is deliberately not watchable. It ships in this repository and runs against
your own gateway with no AWS; see [Running the console](#running-the-console).

That screenshot is a local run: `ws://127.0.0.1:8080`, six seeded paths, and the namespaces sidebar
reading `unavailable` because it discovers gateways through Prometheus, which a local run does not
have. Everything else works without it.

### Per-database usage

![The console's usage tile for one database: connections, storage, downloads, load and quota, each
with a caption naming what it does and does not measure](console/usage-panel.png)

One database at a time — the one the console is connected to, because a socket belongs to one
database for its whole life. The captions are the point. **Downloads is measured before TLS**: the
gateway counts what it hands the load balancer, TLS and TCP happen beyond it, and the egress bill is
1.0–1.3× the number shown depending on traffic shape. **Storage is live data only** — history is
retention, and billing it would charge you for our durability window. **Connections includes the
console itself**, because it is a connection. **Load** is the database's share of the shard's write
lock, and **quota** says whether the limit is the shard's default or one set for this database.

What it does not cover is written on it: writes and storage are isolated per database, fanout CPU is
not — that one is per gateway and does not divide by database.

---

## What's in here

Everything below is in this repository — the server, both clients, the admin UI, the
infrastructure, and the tests that hold it together.

| | |
|---|---|
| **`src/`** | The gateway. Protocol types and validation, the write pipeline (group commit, compare-and-swap, idempotent replay), two storage backends, and the cross-gateway fanout. |
| **`sdk-kotlin/`** | A plain-JVM client: connection state machine, subscriptions, the two-layer mirror, and a Firebase-shaped surface. No Android dependency. |
| **`sdk-android/`** | Android bindings — main-thread callbacks, background ping cadence, reconnect driven by `ConnectivityManager` — plus **a working demo app**. |
| **`console/`** | **A web admin console.** Sign in, browse and edit the live tree, manage users with owner/editor/viewer roles, reset passwords, read the audit log. Served by a small auth server that mints short-lived tokens. |
| **`deploy/`** | Terraform for the whole footprint: gateways behind a network load balancer, Postgres, Redis, container registries, and a Prometheus + Grafana host with dashboards. |
| **`PROTOCOL.md`** | The wire protocol, frozen at v1.6 — frames, the reconnect contract, and the semantics every SDK must implement. Enough to write your own client against. |
| **`test/`, `harness/`** | 324 tests plus an eleven-scenario chaos suite that **kills the server during live traffic** (below). |
| **`scripts/`** | Operator tools: a load rig that drives thousands of connections from forked workers, token minting, a Postgres wait-event sampler, and a harness that measures the commit cycle under fanout load. |

### The chaos suite is the part worth looking at

Eleven scenarios, each one an argument the implementation has to survive rather than a unit
assertion. Among them: an ack that dies *after* the commit, and the replay returning the original
revision instead of writing twice; five clients racing one compare-and-swap, with exactly one ack
and four rejections carrying the state that won; a slow consumer whose queue overflows, forcing the
server to order a resync; a tombstone refusing to let a stale delta resurrect a deleted subtree; and
`SIGKILL` on the gateway mid-traffic, after which clients must back off, reconnect, replay their
pending writes and converge on the same tree.

```bash
npm run chaos
```

### Capacity is measured, not claimed

The write path's ceiling is one Postgres row: a revision counter held under `FOR UPDATE` across the
transaction. Sampling `pg_stat_activity` during a real stall put a backend blocked on that row in
**96% of samples**, and `ρ = λ × S` over an independently counted lock rate gives a hold of
**≥ 7.74 ms** — a ceiling near **129 lock acquisitions per second**, against a measured demand of
124. Group writes amortise it (five writes per acquisition in that run); compare-and-swap commits
alone and does not.

Two consequences worth knowing before you scale anything: **adding gateways does not raise write
throughput**, because they contend for the same row, and the database was never the constraint
(26% CPU, sub-2ms write latency at that load). The lever that pays is statements per transaction.

### Customising the SDK, and shipping your own

Apache-2.0, so forking the SDK, renaming it and publishing it under your own coordinates is
expressly allowed — patent grant included. Two things the build does not do for you yet:

- **Publishing targets only your local Maven repository.** `publishToMavenLocal` works today;
  releasing to Maven Central needs a `publishing.repositories` block and artifact signing, which
  this repo deliberately does not carry.
- **`sdk-kotlin` verifies that its sources jar contains nothing outside its own package.** If you
  rename the package, relax that check or it will fail your build — which is the point of it.

The client is not tied to this repository's deployment: it speaks the protocol to any `wss://` URL.

---

## How it works

```
   client ──wss──┐
   client ──wss──┤──►  gateway (stateless, N instances)
   client ──wss──┘         │
                           ├──►  Postgres    nodes   (materialized current tree)
                           │                 oplog   (append-only event log)
                           │
                           └──►  Redis Streams  (one gateway's writes reach the others)
```

A write is validated, then committed to Postgres and appended to the fanout stream **in one
transaction** — so a delivered notification always has data behind it, and committed data always
gets announced. Every write gets a monotonic revision (`rev`), and the Redis stream id *is* that
rev, which makes "how far behind am I?" a subtraction rather than a reconciliation.

Clients keep a local mirror rendered as `server state ⊕ pending writes`, so your own writes show up
immediately and are reconciled (or replayed) when the connection comes back.

---

## Running the server

Requires **Node 22+**.

```bash
git clone https://github.com/PrabhatBiswal/rtdb.git
cd rtdb
npm ci
```

**Every command below runs from the repository root.** The sources are executed directly through
`tsx`, which is a local dependency — run one of them from somewhere else and Node reports
`Cannot find package 'tsx'`, which reads like a missing install rather than a wrong directory.

### Locally, with no infrastructure at all

```bash
RTDB_PORT=8080 node --import tsx src/gateway/main.ts
```

That gives you a working gateway on in-memory storage — enough to point an app at and develop
against. It will warn that `RTDB_DEV_SECRET` is unset, which means tokens are verified with a
default secret that is public knowledge (it is in this repo). Fine locally, never exposed.

### With Postgres, which is what a real deployment means

```bash
export RTDB_DEV_SECRET="$(openssl rand -hex 32)"   # required — see Authentication
export RTDB_RULES=rules/own-subtree.ts             # required with Postgres — the gateway refuses to boot without a rules module
export RTDB_STORAGE=postgres
export RTDB_PG_URL="postgres://user:pass@host:5432/rtdb"
export RTDB_PORT=8080
export RTDB_ADMIN_PORT=9090
node --import tsx src/gateway/main.ts
```

The tables are created on first connect — there is no migration step to run.

To run **more than one gateway**, give them all the same Redis and the same Postgres:

```bash
export RTDB_REDIS_URL="redis://host:6379"
```

Without Redis a single gateway is fully correct; it just cannot tell a second one what changed.

### Configuration

| Variable | Default | Meaning |
|---|---|---|
| `RTDB_PORT` | `0` (ephemeral) | WebSocket listener port |
| `RTDB_ADMIN_PORT` | off | `/metrics` (Prometheus) and `/healthz` on a separate port. Never publish this one. |
| `RTDB_STORAGE` | `memory` | `memory` or `postgres` |
| `RTDB_PG_URL` | — | Required when `RTDB_STORAGE=postgres` |
| `RTDB_PG_POOL` | `10` | Postgres connections per gateway |
| `RTDB_PG_SCHEMA` | `public` | Lets several shards share one database |
| `RTDB_REDIS_URL` | off | Enables cross-gateway fanout. Unreachable at boot is a boot failure, never a silent island. |
| `RTDB_SHARD` | `0` | Lets several independent shards share one Redis |
| `RTDB_DEV_SECRET` | — | HMAC secret for token verification. **Required with Postgres.** |
| `RTDB_RULES` | off (`allowAll`) | Path to a rules module, resolved against the working directory. **Required with Postgres**; see [Authentication](#authentication). |
| `RTDB_PRUNE_MS` | off | History retention sweep interval (Postgres only) |
| `RTDB_LOCK_TTL_MS` | `3000` | Leader-election lock TTL for the fanout dispatcher |
| `RTDB_PERSIST` | off | File path for memory storage to persist to |
| `RTDB_LIMITS` | `{}` | JSON patch over the protocol's default limits |

`/healthz` reaches storage on every call, so it is a readiness check and not just a liveness one —
use it as your load balancer's health check.

### Docker

```bash
docker build -t rtdb-gateway .
docker run --rm -p 8080:8080 \
  -e RTDB_PORT=8080 \
  -e RTDB_DEV_SECRET="$(openssl rand -hex 32)" \
  rtdb-gateway
```

The image runs the TypeScript sources directly (no build step) and handles `SIGTERM` itself: on
`docker stop` it closes the listener, then the database pool and Redis connections, then exits 0.

### The whole stack on a laptop

`deploy/compose.prod.yml` is the production shape, minus AWS: **two** gateways on one shard, a real
Postgres, a real Redis, and Prometheus + Grafana with the dashboards already provisioned. It is the
fastest way to see fanout, leader election and the metrics doing something.

```bash
export RTDB_DEV_SECRET="$(openssl rand -hex 32)"
docker compose -f deploy/compose.prod.yml up --build -d
```

Gateways on `ws://127.0.0.1:8081` and `:8082`, Grafana on `:3000` (`admin` / `$GRAFANA_PASSWORD`),
Prometheus on `:9090`. Postgres is published on **5433** and Redis on **6380** so neither collides
with one you already run. The health checks call the real `/healthz`, which reaches storage — stop
Postgres and the containers go unhealthy, which is the point of them.

This is also the rig the load tools expect:

```bash
node --import tsx scripts/loadsim.ts --gateways ws://127.0.0.1:8081,ws://127.0.0.1:8082
```

### On AWS, with Terraform

`deploy/` provisions the whole thing — gateways behind a network load balancer, RDS Postgres,
Redis, ECR repositories, and a Prometheus + Grafana host. Everything account-specific is a
variable, and your AWS account id is derived from your credentials rather than configured.

```bash
cd deploy
terraform init
terraform apply \
  -var region=... \
  -var vpc_id=... \
  -var subdomain=rtdb.example.com \
  -var db_password=... \
  -var jwt_secret=... \
  -var gateway_image_tag=... \
  -var ops_image_tag=... \
  -var console_image_tag=...
```

`vpc_id`, `subdomain` and `console_subdomain` have no defaults on purpose — Terraform prompts for
them rather than letting you deploy into someone else's shape by accident.

**One manual step:** the ACM certificate is created with DNS validation, and Terraform does not own
your DNS. `apply` will wait at the certificate while you add the validation CNAME it prints at your
DNS provider — any provider, Route 53 not required. Once the certificate is issued, `apply`
continues on its own.

The two `deploy/iam-*.json` files are the least-privilege policies the deploying principal needs.
They are reference documents: replace `<ACCOUNT_ID>` with your own account id before attaching
them. They are deliberately left un-substitutable so an unedited paste fails instead of quietly
creating a policy that points somewhere else.

---

## Authentication

Tokens are HS256 JWTs. The gateway verifies them with `RTDB_DEV_SECRET` and takes the `sub` claim
as the user id.

**Authorization is a function you write, and there is no rules language.** If you are replacing
Firebase, this is the half of it that is not here: Security Rules are a language with `.read`,
`.write`, `$wildcards` and cascading; what you get here is a TypeScript function and the places it
is called from.

```bash
RTDB_RULES=rules/own-subtree.ts node --import tsx src/gateway/main.ts
```

The module exports `rules` — a function taking the subject, the operation and the path, called once
per write and once per `listen`, never per delta. `rules/own-subtree.ts` is a working example: each
user may read and write only under their own id.

**A deployment on Postgres refuses to start without it.** Without rules the gateway runs `allowAll`,
and then authentication is the only thing between a token and the tree: any client that can connect
can write any path. That includes paths nobody declared — **a namespace is not declared anywhere**,
it is the first segment of a path and it exists as soon as something is written under it, so the
server cannot refuse an "unknown" namespace on its own. Your rules are where it becomes able to. In
memory storage the same situation is a warning rather than a refusal, so trying the project out
stays one command.

One thing to know before you write your own: **console sessions must be let through.** They carry
`console-…` subjects, which match no user's subtree, so a naive own-subtree rule locks the console
out of the tree it administers. What a console session may write is already decided by an invariant
in `src/pipeline/rules.ts` that runs *before* your rules and that no rules module can vote down.

In production, mint tokens in your own backend or IdP with the same secret — the gateway only ever
verifies, it never issues. For local work there is a helper:

```bash
node --import tsx scripts/console-token.ts --name alice --hours 1
```

The gateway **refuses to start** with `RTDB_STORAGE=postgres` unless `RTDB_DEV_SECRET` is set. The
default secret is a literal in this repository's source, so a real deployment that fell back to it
would accept a forged token for any user. With in-memory storage the same situation is a loud
warning instead of a refusal, so trying the project out stays a one-liner.

---

## Running the console

The console is a single HTML page plus a small auth server. It signs in against a user store, mints
short-lived tokens, and talks to the gateway over the same protocol any client uses — it has no
privileged back channel.

By default the store is an AWS SSM parameter, which is right for the deployment in `deploy/` and
wrong for everyone else. Set `CONSOLE_STORE_DIR` and it becomes a file in that directory instead,
with no AWS involved at all.

```bash
# 1. the gateway, with a secret of your own
export RTDB_DEV_SECRET="$(openssl rand -hex 32)"
RTDB_PORT=8080 node --import tsx src/gateway/main.ts

# 2. the first owner. Interactive by design: the password is typed at a prompt with echo off, and
#    never becomes an argument, an env var or a line in your shell history.
export CONSOLE_STORE_DIR="$PWD/.console-store"
node --import tsx scripts/console-admin-set.ts --email you@example.com --role owner

# 3. the console, on the same secret as the gateway
CONSOLE_WSS=ws://127.0.0.1:8080 PORT=8788 node console/auth-server.mjs
```

Open `http://127.0.0.1:8788`, sign in, type a path and press Watch. Expanding a node subscribes to
it and collapsing unsubscribes, so the console is subject to the same `SNAPSHOT_MAX` limit as any
other client — the root is deliberately not listenable.

**One secret, one source.** The console signs tokens the gateway has to verify, so both read
`RTDB_DEV_SECRET`. Give them different values and every token is rejected.

**Three roles.** `owner` manages users and writes; `editor` writes; `viewer` reads. Console subjects
are prefixed (`console-…`, `console-rw-…`) so a rule can tell console traffic from app traffic, and
app tokens are never affected by a console role change.

**The namespaces sidebar needs Prometheus.** It discovers gateways through Prometheus' targets API
and reads the node list from each gateway's admin port, so on a local run without Prometheus it
reads `unavailable`. Everything else — sign-in, watching paths, editing, user management — works
without it. Set `PROM_URL` if you have one.

---

## Using it from Android

Publish the SDK to your local Maven repository:

```bash
cd sdk-kotlin  && ./gradlew publishToMavenLocal
cd sdk-android && ./gradlew publishToMavenLocal
```

Then depend on it:

```kotlin
repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("com.hobostays.rtdb:rtdb-android:0.1.0")
    implementation("com.hobostays.rtdb:rtdb-kotlin-core:0.1.0")
}
```

### Connecting

`AndroidRtdb.create` builds the client and hands it the three things only Android can tell it: the
main thread for callbacks, the network coming back, and the app going to the background (which
relaxes the ping interval).

```kotlin
val client = AndroidRtdb.create(
    context,
    ConnectionOptions(
        url = "wss://your-gateway.example.com",
        token = yourJwt,
    ),
)
client.connect()
```

**One client, one socket.** Use top-level path segments as namespaces rather than opening several
clients — the socket is multiplexed across every listener you add.

### Reading and writing

```kotlin
val room = client.ref("rooms/12")

room.addValueEventListener(object : ValueEventListener {
    override fun onDataChange(snapshot: DataSnapshot) {
        val price = snapshot.child("price")
        // ...
    }
    override fun onCancelled(error: RtdbError) { /* RULES, BADPATH, TOOBIG */ }
})

room.child("price").setValue(JsonPrimitive(900)) { result ->
    when (result) {
        is WriteResult.Committed -> result.rev        // acked; identical for a replayed duplicate
        is WriteResult.Rejected  -> result.value      // lost a compare-and-swap; here is what won
        is WriteResult.Failed    -> result.error      // rejected outright, never auto-retried
    }
}

room.updateChildren(mapOf("price" to JsonPrimitive(900), "beds" to JsonPrimitive(2)))
room.child("draft").removeValue()
```

Value listeners fire with the full mirrored subtree after the initial snapshot and after every
applied change — whether that change came from the server or from this client's own optimistic
write. They always read the local mirror, never a network round trip.

Child events (`addChildEventListener`) and a coroutines `Flow` (`ref.values()`) are both available.
Child events have a Flow too — `ref.childEvents(): Flow<ChildEvent>`. It is not conflated: a value
stream can skip to the latest, a child stream is a sequence and a dropped event never comes back.

### Contended values

For a value two clients may change at once — a counter, a seat, a balance — read the revision you
saw and write against it. A stale write is rejected with the current state rather than silently
overwriting, and you retry from there. This is the same compare-and-swap the server uses
internally, exposed to the client.

---

## Development

```bash
npm run check        # typecheck + the unit and integration battery
npm run test:pg      # against a real Postgres
npm run test:bus     # multi-gateway fanout, needs Redis
npm run chaos        # SIGKILL a gateway mid-traffic and assert nothing is lost

cd sdk-kotlin && ./gradlew soak   # 50 SDK clients, 20 minutes, sockets cut every 30s (§5.36)
```

The chaos suite is the interesting one: it kills gateways during live traffic and asserts that
clients back off, reconnect, replay their pending writes, and converge on the same tree.

The soak is the slow one, and it is deliberately NOT part of `./gradlew test`: 50 Kotlin clients
with value and `childEvents()` listeners, five of them writing, three sockets killed from outside
every thirty seconds, for twenty minutes. It asserts what only time can break — every mirror still
equals the server, no child state delivered twice to a reading client, and a mirror bounded by the
data rather than by the run — and reports reconnects, heap, gateway RSS and cross-client latency.
Its shape is all system properties, so `./gradlew soak -Dsoak.clients=10 -Dsoak.minutes=2` is a
smoke; `SoakTest.kt` lists the rest.

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).

The published SDK artifacts declare the same license in their POM metadata.
