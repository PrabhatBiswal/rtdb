# Security

## Reporting a vulnerability

Use GitHub's private vulnerability reporting on this repository (**Security → Report a
vulnerability**), or email **prabhatbiswal.work@gmail.com**. Please do not open a public issue for
anything exploitable.

Include what you did, what happened, and what you expected. A reproduction against a local gateway
(`npm ci` then `RTDB_PORT=8080 node --import tsx src/gateway/main.ts`) is worth more than a
description. There is no bounty; there will be an acknowledgement and a fix.

Supported: `main`. There are no released versions yet, so there is nothing else to backport to.

## Known and deliberate — please do not report these as vulnerabilities

These are documented design positions, not oversights. Disagreeing with one is an issue, not an
advisory.

- **There is no rules language, and the default authorizes everything.** `allowAll` is the only
  implementation that ships. Any token the gateway accepts can read and write the whole tree until
  you supply your own `Rules`. The README says this in the Authentication section.
- **`RTDB_DEV_SECRET` falls back to a literal in this source.** With `RTDB_STORAGE=postgres` the
  gateway refuses to start without a real one; with in-memory storage it warns loudly and continues,
  so that trying the project out stays a one-liner. An exposed memory-storage gateway will accept a
  forged token for any user.
- **Read authorization is evaluated once, at `listen` time** (`PROTOCOL.md` §3, normative). Topic
  membership *is* the authorization for the life of that subscription, so a rules change or a ban
  does not affect a live subscription until it reconnects. Forced revocation is the admin `kick`
  (§10), which closes the connection. Per-delta evaluation would require per-connection encoding and
  would end the fanout architecture.
- **The admin port is unauthenticated.** `RTDB_ADMIN_PORT` serves `/metrics` and `/healthz` and is
  meant for a private network. Publishing it is a deployment error; the README says never to.
- **Tokens are verified, never issued.** The gateway has no user database and no session store. An
  expired or forged token is rejected at connect; everything else about identity is your IdP's job.

A genuine finding against any of the above — `allowAll` being bypassed where a real `Rules` is
configured, the `console-rw-` write guard being reachable by an app token, a path escaping its
validation, a token accepted past its `exp` — is exactly what this file is for.
