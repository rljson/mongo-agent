#!/usr/bin/env node

/**
 * Integration Test: Agent Server APIs
 * Tests the Fastify routes and agent functionality
 */

import http from 'node:http';


const AGENT_A_URL = process.env.AGENT_A_URL || 'http://localhost:3001';
const AGENT_B_URL = process.env.AGENT_B_URL || 'http://localhost:3002';
const HUB_URL = process.env.HUB_URL || 'http://localhost:3200';

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
function header(msg: string): void {
  console.log(`\n${colors.cyan}${'═'.repeat(70)}${colors.reset}`);
  console.log(`  ${msg}`);
  console.log(`${colors.cyan}${'═'.repeat(70)}${colors.reset}\n`);
}

interface HttpResponse<T = unknown> {
  status: number;
  data: T;
}

async function httpGet<T = unknown>(url: string): Promise<HttpResponse<T>> {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    const options: http.RequestOptions = {
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
          resolve({ status: res.statusCode!, data: JSON.parse(data) });
        } catch {
          resolve({ status: res.statusCode!, data: data as unknown as T });
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

async function httpPost<T = unknown>(
  url: string,
  body: Record<string, unknown> = {},
): Promise<HttpResponse<T>> {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    const postData = JSON.stringify(body);

    const options: http.RequestOptions = {
      hostname: urlObj.hostname,
      port: urlObj.port,
      path: urlObj.pathname + urlObj.search,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(postData),
      },
    };

    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode!, data: JSON.parse(data) });
        } catch {
          resolve({ status: res.statusCode!, data: data as unknown as T });
        }
      });
    });

    req.on('error', reject);
    req.setTimeout(5000, () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });
    req.write(postData);
    req.end();
  });
}

let testsRun = 0;
let testsPassed = 0;
let testsFailed = 0;

function assertEqual<T>(actual: T, expected: T, message: string): void {
  testsRun++;
  if (actual === expected) {
    success(message);
    testsPassed++;
  } else {
    error(`${message} (expected: ${expected}, got: ${actual})`);
    testsFailed++;
  }
}

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

interface HealthResponse {
  ok: boolean;
  nodeId?: string;
}

interface SyncInfoResponse {
  nodeId: string;
  headSeq: number;
  headHash: string;
}

interface SyncStateResponse {
  ok: boolean;
  origin: string;
  lastSeqPulled: number;
  state?: unknown;
}

interface SyncPullResponse {
  ops: unknown[];
  hasMore?: boolean;
}

interface ErrorResponse {
  error: string;
}

interface ClientInfo {
  clientId: string;
  url: string;
  lastSeenAt: string;
}

interface ClientsResponse {
  clients: ClientInfo[];
}

async function testAgentAPIs(): Promise<void> {
  header('Agent Server API Tests');

  // Test Agent A Health
  info('Testing Agent A health endpoint...');
  try {
    const res = await httpGet<HealthResponse>(`${AGENT_A_URL}/health`);
    assertEqual(res.status, 200, 'Agent A /health returns 200');
    assert(res.data.ok === true, 'Agent A health ok is true');
    assert(res.data.nodeId === 'nodeA', 'Agent A health returns nodeId');
  } catch (e) {
    const err = e as Error;
    error(`Agent A health check failed: ${err.message}`);
    testsFailed++;
    testsRun++;
  }

  // Test Agent B Health
  info('Testing Agent B health endpoint...');
  try {
    const res = await httpGet<HealthResponse>(`${AGENT_B_URL}/health`);
    assertEqual(res.status, 200, 'Agent B /health returns 200');
    assert(res.data.ok === true, 'Agent B health ok is true');
    assert(res.data.nodeId === 'nodeB', 'Agent B health returns nodeId');
  } catch (e) {
    const err = e as Error;
    error(`Agent B health check failed: ${err.message}`);
    testsFailed++;
    testsRun++;
  }

  // Test Hub Health
  info('Testing Hub health endpoint...');
  try {
    const res = await httpGet<HealthResponse>(`${HUB_URL}/health`);
    assertEqual(res.status, 200, 'Hub /health returns 200');
    assert(res.data.ok === true, 'Hub health ok is true');
  } catch (e) {
    const err = e as Error;
    error(`Hub health check failed: ${err.message}`);
    testsFailed++;
    testsRun++;
  }

  // Test Agent A Sync Info
  info('Testing Agent A /sync/info endpoint...');
  try {
    const res = await httpGet<SyncInfoResponse>(`${AGENT_A_URL}/sync/info`);
    assertEqual(res.status, 200, 'Agent A /sync/info returns 200');
    assert(res.data.nodeId === 'nodeA', 'Agent A nodeId is nodeA');
    assert(typeof res.data.headSeq === 'number', 'Agent A headSeq is a number');
    assert(
      typeof res.data.headHash === 'string',
      'Agent A headHash is a string',
    );
    info(
      `  Agent A: seq=${res.data.headSeq}, hash=${res.data.headHash.slice(0, 8)}...`,
    );
  } catch (e) {
    const err = e as Error;
    error(`Agent A /sync/info failed: ${err.message}`);
    testsFailed++;
    testsRun++;
  }

  // Test Agent B Sync Info
  info('Testing Agent B /sync/info endpoint...');
  try {
    const res = await httpGet<SyncInfoResponse>(`${AGENT_B_URL}/sync/info`);
    assertEqual(res.status, 200, 'Agent B /sync/info returns 200');
    assert(res.data.nodeId === 'nodeB', 'Agent B nodeId is nodeB');
    assert(typeof res.data.headSeq === 'number', 'Agent B headSeq is a number');
    assert(
      typeof res.data.headHash === 'string',
      'Agent B headHash is a string',
    );
    info(
      `  Agent B: seq=${res.data.headSeq}, hash=${res.data.headHash.slice(0, 8)}...`,
    );
  } catch (e) {
    const err = e as Error;
    error(`Agent B /sync/info failed: ${err.message}`);
    testsFailed++;
    testsRun++;
  }

  // Test Agent A Sync State
  info('Testing Agent A /sync/state/:origin endpoint...');
  try {
    const res = await httpGet<SyncStateResponse>(
      `${AGENT_A_URL}/sync/state/nodeB`,
    );
    assertEqual(res.status, 200, 'Agent A /sync/state/nodeB returns 200');
    assert(res.data.ok === true, 'Response has ok: true');
    assert(res.data.origin === 'nodeB', 'Origin is nodeB');
    assert(
      typeof res.data.lastSeqPulled === 'number',
      'lastSeqPulled is a number',
    );
    info(
      `  Agent A knows about nodeB: lastSeqPulled=${res.data.lastSeqPulled}`,
    );
  } catch (e) {
    const err = e as Error;
    error(`Agent A /sync/state/nodeB failed: ${err.message}`);
    testsFailed++;
    testsRun++;
  }

  // Test POST /sync/pull
  info('Testing Agent A /sync/pull endpoint...');
  try {
    const res = await httpPost<SyncPullResponse>(`${AGENT_A_URL}/sync/pull`, {
      origin: 'nodeB',
      lastSeqSeen: 0,
    });
    assertEqual(res.status, 200, 'Agent A /sync/pull returns 200');
    assert(Array.isArray(res.data.ops), 'Response has ops array');
    info(`  Agent A pulled ${res.data.ops.length} operations from nodeB`);
  } catch (e) {
    const err = e as Error;
    error(`Agent A /sync/pull failed: ${err.message}`);
    testsFailed++;
    testsRun++;
  }

  // Test POST /sync/pull with missing origin
  info('Testing Agent A /sync/pull with missing origin (should fail)...');
  try {
    const res = await httpPost<ErrorResponse>(`${AGENT_A_URL}/sync/pull`, {});
    assertEqual(
      res.status,
      400,
      'Agent A /sync/pull without origin returns 400',
    );
    assert(!!res.data.error, 'Response contains error message');
  } catch (e) {
    const err = e as Error;
    error(`Agent A /sync/pull validation test failed: ${err.message}`);
    testsFailed++;
    testsRun++;
  }
}

async function testHubRelay(): Promise<void> {
  header('Hub Relay Tests');

  info('Testing Hub /hub/clients endpoint...');
  try {
    const res = await httpGet<ClientsResponse>(`${HUB_URL}/hub/clients`);
    assertEqual(res.status, 200, 'Hub /hub/clients returns 200');
    assert(Array.isArray(res.data.clients), 'Response has clients array');

    // Check if agents registered
    const clientIds = res.data.clients.map((n) => n.clientId);
    info(`  Hub knows about clients: ${clientIds.join(', ')}`);

    const hasNodeA = clientIds.includes('nodeA');
    const hasNodeB = clientIds.includes('nodeB');

    assert(hasNodeA || hasNodeB, 'At least one agent registered with hub');

    if (hasNodeA) {
      success('nodeA is registered with hub');
    }
    if (hasNodeB) {
      success('nodeB is registered with hub');
    }
  } catch (e) {
    const err = e as Error;
    error(`Hub /hub/clients failed: ${err.message}`);
    testsFailed++;
    testsRun++;
  }

  info('Testing Hub relay to nodeA /sync/info...');
  try {
    const res = await httpGet<SyncInfoResponse>(
      `${HUB_URL}/hub/relay/nodeA/sync/info`,
    );
    assertEqual(res.status, 200, 'Hub relay to nodeA /sync/info returns 200');
    assert(res.data.nodeId === 'nodeA', 'Relayed response has nodeA');
    assert(
      typeof res.data.headSeq === 'number',
      'Relayed response has headSeq',
    );
    info(`  Relayed nodeA info: seq=${res.data.headSeq}`);
  } catch (e) {
    const err = e as Error;
    error(`Hub relay to nodeA failed: ${err.message}`);
    testsFailed++;
    testsRun++;
  }
}

async function main(): Promise<void> {
  console.log('\n' + '═'.repeat(70));
  console.log('  Agent Server Integration Tests');
  console.log('═'.repeat(70));
  console.log();

  info(`Testing against:`);
  info(`  Agent A: ${AGENT_A_URL}`);
  info(`  Agent B: ${AGENT_B_URL}`);
  info(`  Hub:     ${HUB_URL}`);

  try {
    await testAgentAPIs();
    await testHubRelay();

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
