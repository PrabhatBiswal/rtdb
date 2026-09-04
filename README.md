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
| **`PROTOCOL.md`** | The wire protocol, frozen at v1.5 — frames, the reconnect contract, and the semantics every SDK must implement. Enough to write your own client against. |
| **`test/`, `harness/`** | 230 tests, including **a chaos suite that kills the server during live traffic** (below). |
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
npm ci
```

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
  -var jwt_secret=...
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
as the user id; path-level rules are evaluated per subscription and per write.

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
```

The chaos suite is the interesting one: it kills gateways during live traffic and asserts that
clients back off, reconnect, replay their pending writes, and converge on the same tree.

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).

The published SDK artifacts declare the same license in their POM metadata.
