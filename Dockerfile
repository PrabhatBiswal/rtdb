# WORKLOAD §2. node:22-slim, NOT alpine: uWebSockets.js ships glibc prebuilds and there is no
# musl build to fall back to. Build on arm64 — the gateways are t4g (Graviton) instances.
FROM node:22-slim AS deps
WORKDIR /app
# uWebSockets.js is a `github:` dependency; npm resolves it from codeload but falls back to a git
# clone, and a build that depends on which path npm picked today is not a build.
RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
# RDS refuses unencrypted connections (rds.force_ssl), and `sslmode=require` alone encrypts without
# checking who is on the other end. The gateway connects with verify-full, which needs Amazon's RDS
# CA bundle in the image. Fetched at build rather than committed: the bundle gains CAs over time and
# a stale committed copy is exactly what breaks on an RDS CA rotation.
RUN curl -fsSL -o /rds-global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem \
 && test -s /rds-global-bundle.pem
COPY package.json package-lock.json ./
# tsx is a runtime dependency here, not a dev one: this image runs the TypeScript sources directly,
# exactly as every test, the chaos runner and the restore drill already do. `--omit=dev` therefore
# still leaves a complete runtime and drops the compiler and the type packages.
RUN npm ci --omit=dev && npm cache clean --force

FROM node:22-slim
ENV NODE_ENV=production
WORKDIR /app
COPY --from=deps /rds-global-bundle.pem /etc/ssl/rds/global-bundle.pem
COPY --from=deps /app/node_modules ./node_modules
COPY package.json ./
COPY src ./src
# 8080 = the WSS listener the NLB targets; 9090 = /metrics + /healthz, never published (§2).
EXPOSE 8080 9090
USER node
# node runs as PID 1 and installs its own SIGTERM handler (main.ts): `docker stop` closes the
# listen socket, then the pool and the Redis connections, then exits 0. No init shim needed.
CMD ["node", "--import", "tsx", "src/gateway/main.ts"]
