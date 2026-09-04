import { after } from 'node:test';
import { storageSemantics } from '../storage-suite.ts';
import { createDatabase } from './helper.ts';

// The WP1 conformance suite, reused VERBATIM (WORKLOAD §4): one database for the file, one schema
// per `fresh()` store. Nothing here may adapt the suite to the implementation.
const db = await createDatabase('conformance');
after(() => db.drop());

storageSemantics('PostgresStorage', (limits) => db.make(limits));
