import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { existsSync, rmSync } from 'node:fs';
import { mkdtemp } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

/**
 * The brew-installed BINARY, spawned per test on an ephemeral port in a temp dir. Never the brew
 * SERVICE (WORKLOAD §0.8): the chaos suite SIGKILLs Redis, and killing a server we did not start is
 * the same class of mistake as dropping a database we did not create.
 */
const BREW_BIN = '/opt/homebrew/bin/redis-server';
export const REDIS_BIN =
  process.env['RTDB_REDIS_BIN'] ?? (existsSync(BREW_BIN) ? BREW_BIN : 'redis-server'); // CI installs it on PATH

/** Everything this process ever spawned, so a suite can prove it left nothing behind. */
const spawned: RedisProcess[] = [];

/** An OS-assigned free port. Racy in principle; #spawn retries, which is cheaper than a port pool. */
const freePort = async (): Promise<number> => {
  const s = createServer();
  s.listen(0, '127.0.0.1');
  await once(s, 'listening');
  const { port } = s.address() as { port: number };
  await new Promise((r) => s.close(r));
  return port;
};

export class RedisProcess {
  #child: ChildProcess | null = null;
  port = 0;
  #dir = '';

  static async start(): Promise<RedisProcess> {
    const r = new RedisProcess();
    r.#dir = await mkdtemp(join(tmpdir(), 'rtdb-redis-'));
    spawned.push(r);
    await r.#spawn();
    return r;
  }

  get url(): string {
    return `redis://127.0.0.1:${this.port}`;
  }

  get dir(): string {
    return this.#dir;
  }

  get pid(): number | undefined {
    return this.#child?.pid;
  }

  async #spawn(port = 0): Promise<void> {
    for (let attempt = 0; ; attempt++) {
      this.port = port || (await freePort());
      // `--save ''` + no appendonly: nothing on disk, so a SIGKILLed Redis comes back EMPTY. That is
      // the honest shape of the failure the consumer has to survive (§8 replay-or-resync).
      const child = spawn(
        REDIS_BIN,
        ['--port', String(this.port), '--bind', '127.0.0.1', '--dir', this.#dir, '--save', '', '--appendonly', 'no'],
        { stdio: ['ignore', 'pipe', 'ignore'] },
      );
      const ready = await new Promise<boolean>((resolve, reject) => {
        let out = '';
        child.stdout?.setEncoding('utf8');
        child.stdout?.on('data', (chunk: string) => {
          out += chunk;
          if (/Ready to accept connections/.test(out)) resolve(true);
          if (/Could not create server TCP|Address already in use/.test(out)) resolve(false);
        });
        child.once('exit', (code) => reject(new Error(`redis-server exited before listening (code ${code})`)));
      }).catch((e: unknown) => {
        if (port || attempt >= 4) throw e;
        return false; // an ephemeral port lost its race; pick another
      });
      if (ready) {
        child.stdout?.removeAllListeners('data');
        child.stdout?.resume(); // drain the log, or a full pipe buffer eventually blocks the server
        this.#child = child;
        return;
      }
      child.kill('SIGKILL');
      if (port || attempt >= 4) throw new Error(`redis-server could not bind port ${this.port}`);
    }
  }

  /** SIGKILL by default: the chaos case. The data directory survives, so `restart()` can reuse it. */
  async kill(signal: NodeJS.Signals = 'SIGKILL'): Promise<void> {
    const child = this.#child;
    if (!child) return;
    this.#child = null;
    const exited = once(child, 'exit');
    child.kill(signal);
    await exited;
  }

  /** Same port, so every client's `RTDB_REDIS_URL` stays valid across the outage. */
  async restart(): Promise<void> {
    await this.kill();
    await this.#spawn(this.port);
  }

  async stop(): Promise<void> {
    await this.kill('SIGTERM');
    rmSync(this.#dir, { recursive: true, force: true });
  }
}

/**
 * The same discipline the Postgres suites hold for databases and schemas: a clean run leaves no
 * redis-server alive and no temp dir behind. Called from an `after` hook; throws on either.
 */
export function assertNoLeftovers(): void {
  const leaked = spawned.flatMap((r) => {
    const problems: string[] = [];
    const pid = r.pid;
    if (pid !== undefined) {
      try {
        process.kill(pid, 0);
        problems.push(`redis-server pid ${pid} still alive on port ${r.port}`);
      } catch {
        /* ESRCH: gone, which is the point */
      }
    }
    if (existsSync(r.dir)) problems.push(`temp dir left behind: ${r.dir}`);
    return problems;
  });
  if (leaked.length > 0) throw new Error(`redis harness left state behind:\n  ${leaked.join('\n  ')}`);
}
