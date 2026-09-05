#!/usr/bin/env tsx

/**
 * Integration Test: Hub Relay & syncOriginFromHub
 * Tests that agents sync through the hub relay
 */

import { MongoClient, ObjectId } from 'mongodb';
import http from 'node:http';


const MONGO_A_URI =
  process.env.MONGO_A_URI ||
  'mongodb://localhost:27017/syncdb?directConnection=true';
const MONGO_B_URI =
  process.env.MONGO_B_URI ||
  'mongodb://localhost:27018/syncdb?directConnection=true';
const HUB_URL = process.env.HUB_URL || 'http://localhost:3200';
const WAIT_TIME_MS = 4000;

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
} as const;

function log(color: string, symbol: string, message: string): void {
  console.log(`${color}${symbol}${colors.reset} ${message}`);
}

function success(msg: string): void {
  log(colors.green, '✓', msg);
}
function error(msg: string): void {
  log(colors.red, '✗', msg);
}
function info(msg: string): void {
  log(colors.blue, 'ℹ', msg);
}
function waiting(msg: string): void {
  log(colors.yellow, '⏳', msg);
}
function header(msg: string): void {
  console.log(`\n${colors.cyan}${'═'.repeat(70)}${colors.reset}`);
  console.log(`  ${msg}`);
  console.log(`${colors.cyan}${'═'.repeat(70)}${colors.reset}\n`);
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

interface HttpResponse<T = unknown> {
  status: number;
  data: T;
}

async function httpGet<T = unknown>(url: string): Promise<HttpResponse<T>> {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    const options = {
      hostname: urlObj.hostname,
      port: urlObj.port,
      path: urlObj.pathname + urlObj.search,
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
    };

    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        try {
          resolve({
            status: res.statusCode || 500,
            data: JSON.parse(data) as T,
          });
        } catch {
          resolve({ status: res.statusCode || 500, data: data as T });
        }
      });
    });

    req.on('error', reject);
    req.setTimeout(5000, () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });
    req.end();
  });
}

let testsRun = 0;
let testsPassed = 0;
let testsFailed = 0;

function assert(condition: boolean, message: string): void {
  testsRun++;
  if (condition) {
    success(message);
    testsPassed++;
  } else {
    error(message);
    testsFailed++;
  }
}

interface ClientInfo {
  clientId: string;
  url: string;
}

interface HubClientsResponse {
  clients: ClientInfo[];
}

interface SyncOp {
  _id: ObjectId;
  origin: string;
  seq: number;
  operationType: 'insert' | 'update' | 'delete' | 'replace';
  docId: string;
}

interface SyncState {
  origin: string;
  lastSeqSeen: number;
  applied?: {
    lastSeq: number;
  };
}

async function testHubRegistration(): Promise<void> {
  header('Hub Registration Test');

  info('Checking if agents registered with hub...');

  try {
    const res = await httpGet<HubClientsResponse>(`${HUB_URL}/hub/clients`);
    assert(res.status === 200, 'Hub /hub/clients endpoint is accessible');
    assert(Array.isArray(res.data.clients), 'Hub returns clients array');

    const clientIds = res.data.clients.map((n) => n.clientId);
    info(`  Hub knows about: ${clientIds.join(', ')}`);

    const hasNodeA = clientIds.includes('nodeA');
    const hasNodeB = clientIds.includes('nodeB');

    assert(hasNodeA, 'nodeA is registered with hub');
    assert(hasNodeB, 'nodeB is registered with hub');

    // Check client details
    const nodeA = res.data.clients.find((n) => n.clientId === 'nodeA');
    const nodeB = res.data.clients.find((n) => n.clientId === 'nodeB');

    if (nodeA) {
      assert(typeof nodeA.url === 'string', 'nodeA has url');
      info(`  nodeA: url=${nodeA.url}`);
    }

    if (nodeB) {
      assert(typeof nodeB.url === 'string', 'nodeB has url');
      info(`  nodeB: url=${nodeB.url}`);
    }
  } catch (e) {
    const err = e as Error;
    error(`Hub registration test failed: ${err.message}`);
    testsFailed++;
    testsRun++;
  }
}

async function testHubRelay(): Promise<void> {
  header('Hub Relay Functionality Test');

  let clientA: MongoClient | undefined;
  let clientB: MongoClient | undefined;
  const testId = `hubrelay_${Date.now()}`;

  try {
    info('Connecting to MongoDB instances...');
    clientA = new MongoClient(MONGO_A_URI);
    clientB = new MongoClient(MONGO_B_URI);

    await clientA.connect();
    await clientB.connect();
    success('Connected to both MongoDB instances');

    const dbA = clientA.db('syncdb');
    const dbB = clientB.db('syncdb');
    const collA = dbA.collection('articles');
    const collB = dbB.collection('articles');

    // Get initial operation counts
    info('Getting initial sync_ops counts...');
    const initialOpsA = await dbA
      .collection('sync_ops')
      .countDocuments({ origin: 'nodeA' });
    const initialOpsB = await dbB
      .collection('sync_ops')
      .countDocuments({ origin: 'nodeB' });
    info(`  Initial ops: nodeA=${initialOpsA}, nodeB=${initialOpsB}`);

    // Insert document in A
    info('Inserting document in MongoDB A...');
    const docA = {
      testId,
      source: 'nodeA',
      title: 'Hub Relay Test Document',
      content: 'This should sync through the hub',
      timestamp: new Date(),
    };

    const resultA = await collA.insertOne(docA);
    success(`Inserted document: ${resultA.insertedId}`);

    // Wait for change stream and hub relay
    waiting(`Waiting ${WAIT_TIME_MS}ms for change stream → hub → sync...`);
    await sleep(WAIT_TIME_MS);

    // Note: Hub is a relay only, it doesn't store operations.
    // We verify the relay worked by checking if the document synced to B.

    // Check if document synced to B
    info('Checking if document synced to MongoDB B...');
    const syncedDoc = await collB.findOne({ _id: resultA.insertedId });
    assert(syncedDoc !== null, 'Document synced from A to B through hub');

    if (syncedDoc) {
      assert(syncedDoc.testId === testId, 'Synced document has correct testId');
      assert(
        syncedDoc.source === 'nodeA',
        'Synced document has correct source',
      );
      assert(
        syncedDoc.title === docA.title,
        'Synced document has correct title',
      );
    }

    // Check if operation was recorded in B's sync_ops
    info('Checking if operation was recorded in MongoDB B sync_ops...');
    const remoteOp = await dbB.collection<SyncOp>('sync_ops').findOne({
      origin: 'nodeA',
      docId: resultA.insertedId.toString(),
    });

    assert(remoteOp !== null, 'Remote operation recorded in MongoDB B');
    if (remoteOp) {
      assert(remoteOp.operationType === 'insert', 'Remote op has correct type');
      info(`  Remote op in B: ${remoteOp._id}, seq=${remoteOp.seq}`);
    }

    // Check sync_state in B
    info('Checking sync_state in MongoDB B...');
    const syncState = await dbB
      .collection<SyncState>('sync_state')
      .findOne({ origin: 'nodeA' });
    assert(syncState !== null, 'MongoDB B has sync_state for nodeA');

    if (syncState) {
      assert(
        syncState.applied && syncState.applied.lastSeq > 0,
        `B applied operations from A (applied.lastSeq=${syncState.applied?.lastSeq})`,
      );
      info(
        `  Sync state: lastSeqSeen=${syncState.lastSeqSeen}, applied.lastSeq=${syncState.applied?.lastSeq}`,
      );
    }

    // Cleanup
    info('Cleaning up test document...');
    await collA.deleteMany({ testId });
    await collB.deleteMany({ testId });
  } catch (e) {
    const err = e as Error;
    error(`Hub relay test failed: ${err.message}`);
    console.error(err.stack);
    testsFailed++;
    testsRun++;
  } finally {
    if (clientA) await clientA.close();
    if (clientB) await clientB.close();
  }
}

async function testSyncState(): Promise<void> {
  header('Sync State Tracking Test');

  let clientA: MongoClient | undefined;
  let clientB: MongoClient | undefined;

  try {
    info('Connecting to MongoDB instances...');
    clientA = new MongoClient(MONGO_A_URI);
    clientB = new MongoClient(MONGO_B_URI);

    await clientA.connect();
    await clientB.connect();
    success('Connected to both MongoDB instances');

    const dbA = clientA.db('syncdb');
    const dbB = clientB.db('syncdb');

    // Check sync_state in A for nodeB
    info('Checking sync_state in MongoDB A for nodeB...');
    const stateAB = await dbA
      .collection<SyncState>('sync_state')
      .findOne({ origin: 'nodeB' });

    if (stateAB) {
      success('MongoDB A is tracking sync state for nodeB');
      assert(typeof stateAB.lastSeqSeen === 'number', 'lastSeqSeen is tracked');
      assert(
        stateAB.applied && typeof stateAB.applied.lastSeq === 'number',
        'applied.lastSeq is tracked',
      );
      info(
        `  lastSeqSeen: ${stateAB.lastSeqSeen}, applied.lastSeq: ${stateAB.applied?.lastSeq}`,
      );
    } else {
      info('No sync state yet (nodes might not have synced)');
    }

    // Check sync_state in B for nodeA
    info('Checking sync_state in MongoDB B for nodeA...');
    const stateBA = await dbB
      .collection<SyncState>('sync_state')
      .findOne({ origin: 'nodeA' });

    if (stateBA) {
      success('MongoDB B is tracking sync state for nodeA');
      assert(typeof stateBA.lastSeqSeen === 'number', 'lastSeqSeen is tracked');
      assert(
        stateBA.applied && typeof stateBA.applied.lastSeq === 'number',
        'applied.lastSeq is tracked',
      );
      info(
        `  lastSeqSeen: ${stateBA.lastSeqSeen}, applied.lastSeq: ${stateBA.applied?.lastSeq}`,
      );
    } else {
      info('No sync state yet (nodes might not have synced)');
    }
  } catch (e) {
    const err = e as Error;
    error(`Sync state test failed: ${err.message}`);
    testsFailed++;
    testsRun++;
  } finally {
    if (clientA) await clientA.close();
    if (clientB) await clientB.close();
  }
}

async function main(): Promise<void> {
  console.log('\n' + '═'.repeat(70));
  console.log('  Hub Relay & syncOriginFromHub Integration Tests');
  console.log('═'.repeat(70));
  console.log();

  info(`Testing against:`);
  info(`  MongoDB A: ${MONGO_A_URI}`);
  info(`  MongoDB B: ${MONGO_B_URI}`);
  info(`  Hub:       ${HUB_URL}`);
  info(`  Sync wait: ${WAIT_TIME_MS}ms`);

  try {
    await testHubRegistration();
    await testHubRelay();
    await testSyncState();

    console.log();
    console.log('═'.repeat(70));
    console.log(`  Test Results: ${testsPassed}/${testsRun} passed`);
    if (testsFailed > 0) {
      console.log(`  ${colors.red}${testsFailed} tests failed${colors.reset}`);
    } else {
      console.log(`  ${colors.green}All tests passed!${colors.reset}`);
    }
    console.log('═'.repeat(70));

    process.exit(testsFailed > 0 ? 1 : 0);
  } catch (e) {
    const err = e as Error;
    error(`Fatal error: ${err.message}`);
    console.error(err.stack);
    process.exit(1);
  }
}

main();
