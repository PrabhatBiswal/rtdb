import net from 'node:net';

/**
 * A TCP relay in front of the gateway, so chaos scenarios can break the network without any
 * test-only hooks in the server. Two faults, which between them drive most of Gate D:
 *  - `cut()`   — the connection dies mid-flight (a lost ack is just a cut after the commit);
 *  - `pauseDownstream()` — stop reading server->client, so the kernel window closes and uWS
 *    backpressure builds for real. That is a slow consumer, not a simulation of one.
 */
export class Proxy {
  readonly #pairs = new Set<{ client: net.Socket; upstream: net.Socket }>();
  #paused = false;
  #blocked = false;

  #target: number;

  private constructor(
    private readonly server: net.Server,
    readonly port: number,
    target: number,
  ) {
    this.#target = target;
  }

  static async start(targetPort: number, targetHost = '127.0.0.1'): Promise<Proxy> {
    let self: Proxy;
    const server = net.createServer((client) => {
      if (self.#blocked) return void client.destroy(); // the server is unreachable, not just slow
      const upstream = net.connect(self.#target, targetHost);
      const pair = { client, upstream };
      self.#pairs.add(pair);
      if (self.#paused) upstream.pause();

      client.on('data', (d) => upstream.write(d));
      upstream.on('data', (d) => client.write(d));
      const end = (): void => {
        self.#pairs.delete(pair);
        client.destroy();
        upstream.destroy();
      };
      for (const s of [client, upstream]) {
        s.on('close', end);
        s.on('error', end);
      }
    });
    const port = await new Promise<number>((resolve, reject) => {
      server.on('error', reject);
      server.listen(0, '127.0.0.1', () => resolve((server.address() as net.AddressInfo).port));
    });
    self = new Proxy(server, port, targetPort);
    return self;
  }

  get url(): string {
    return `ws://127.0.0.1:${this.port}`;
  }

  get connections(): number {
    return this.#pairs.size;
  }

  /** Kill every live connection without a close frame — the wire just stops. */
  cut(): void {
    for (const { client, upstream } of [...this.#pairs]) {
      client.destroy();
      upstream.destroy();
    }
    this.#pairs.clear();
  }

  /** Cut, and refuse reconnects — the client backs off against a server that is simply not there. */
  blackhole(): void {
    this.#blocked = true;
    this.cut();
  }

  restore(): void {
    this.#blocked = false;
  }

  /**
   * Send everything from here on to a different gateway, and cut what is connected so it reconnects.
   * This is what the NLB does the moment a gateway dies (§8 ops note) — the client keeps the one URL
   * it was given and finds itself somewhere else.
   */
  retarget(port: number): void {
    this.#target = port;
    this.cut();
  }

  /** Stop draining server->client. Real backpressure builds behind this. */
  pauseDownstream(): void {
    this.#paused = true;
    for (const { upstream } of this.#pairs) upstream.pause();
  }

  resumeDownstream(): void {
    this.#paused = false;
    for (const { upstream } of this.#pairs) upstream.resume();
  }

  stop(): void {
    this.cut();
    this.server.close();
  }
}
