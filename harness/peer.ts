import { createInterface } from 'node:readline';
import { isDeepStrictEqual } from 'node:util';
import type { Json } from '../src/protocol/frames.ts';
import { joinPath } from '../src/protocol/path.ts';
import { RtdbClient } from './client.ts';
import { waitUntil } from './scenario.ts';

/**
 * The WP1 harness client as a scriptable process, so another SDK's tests can put a REAL second
 * protocol citizen on the wire (WORKLOAD §6 Gate C: "a Kotlin client and the TS harness client
 * converge together"). Not a test itself — it is the other half of someone else's test.
 *
 * Usage: node --import tsx harness/peer.ts <url> <token> <path>
 * Protocol: one command per stdin line, one line of output per command.
 *   put <relative|.> <json>     -> `ok`             (waits for the ack)
 *   await <relative|.> <json>   -> `ok`             (waits until the mirror reads that value)
 *   dump                        -> `value <json>`   (the mirrored view at <path>)
 *   exit                        -> exits 0
 * Anything that throws answers `error <message>`, so the driver on the other end sees why.
 */
const [url = '', token = '', root = ''] = process.argv.slice(2);

const client = new RtdbClient({ url, token, pingIntervalMs: 60_000 });
client.connect();
await client.ready();

let snapshotted = false;
client.listen(root, () => {
  snapshotted = true;
});
await waitUntil(() => snapshotted, 'the first snapshot');
process.stdout.write('ready\n');

const resolve = (relative: string): string => (relative === '.' ? root : joinPath(root, relative));
const say = (line: string): void => void process.stdout.write(`${line}\n`);

for await (const line of createInterface({ input: process.stdin })) {
  const [command = '', relative = '.', ...json] = line.trim().split(' ');
  const path = resolve(relative);
  try {
    switch (command) {
      case 'put':
        await client.put(path, JSON.parse(json.join(' ')) as Json);
        say('ok');
        break;

      case 'await': {
        // Structural, not textual: two mirrors agree on a tree, never on a key order.
        const expected = JSON.parse(json.join(' ')) as Json;
        await waitUntil(
          () => isDeepStrictEqual(client.value(path), expected),
          `${path} == ${json.join(' ')} (it was ${JSON.stringify(client.value(path))})`,
          15_000,
        );
        say('ok');
        break;
      }

      case 'dump':
        say(`value ${JSON.stringify(client.value(root))}`);
        break;

      case 'exit':
        client.close();
        process.exit(0);
    }
  } catch (e) {
    say(`error ${e instanceof Error ? e.message : String(e)}`);
  }
}
