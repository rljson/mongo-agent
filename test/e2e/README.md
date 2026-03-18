# End-to-End Tests

This directory contains integration and end-to-end tests that have external dependencies.

## ⚠️ Dependencies

**These tests require:**

- **Docker** and Docker Compose (see [doc/prepare.md](../../doc/prepare.md#docker-engine) for free alternatives to Docker Desktop)
- **MongoDB** (via Docker containers)
- Running sync infrastructure (hub, agents)

## 📋 Test Categories

### 🎯 RLJSON Integration Tests

- `test-rljson-integration.sh` / `test-rljson-integration.ts` - **Complete RLJSON workflow test**
  - Writes test data to MongoDB
  - Scans and converts to RLJSON tree structure
  - Verifies blob storage for document content
  - Validates hash integrity
  - Tests integration with @rljson/db layer
  - Displays converted data and hashes

### 🔄 Sync Tests

- `test-sync.sh` / `test-sync.js` - Basic bidirectional sync verification
- `test-agent-restart.sh` - Agent restart resilience
- `test-offline-sync.sh` - Complex offline sync scenarios
- `test-simple-offline.sh` - Simple offline sync test

### 🔐 Integrity & Hashing Tests

- `test-integrity-hash.js` - Pre-computed integrity hash performance
- `test-state-hash.js` - State hash computation and verification
- `test-state-hash.sh` - State hash wrapper with formatting
- `test-state-quick.js` - Lightweight state hash test
- `benchmark-state-hash.js` - State hash performance benchmarking

### 🛡️ Tamper Detection Tests

- `test-tamper-detection.js` - Tamper detection mechanism
- `test-tamper-repair.js` - Tamper repair workflow
- `test-tamper-demo.sh` - Complete tamper detection demo

### 📊 Dirty Tracking Tests

- `test-dirty-partitions.js` - Incremental update verification

### 🔧 Utility Tests

- `test-restore-and-verify.sh` - Database restore and verification
- `run-all-tests.sh` - Test suite runner

## 🚀 Running Tests

### Prerequisites

```bash
# Start the infrastructure
docker compose up -d

# Verify containers are running
docker compose ps
```

### Running Individual Tests

**RLJSON Integration Test (recommended starting point):**

```bash
cd test/e2e
./test-rljson-integration.sh
# OR run directly with tsx:
npx tsx test/e2e/test-rljson-integration.ts
```

This test demonstrates the complete RLJSON integration and outputs:

- Tree structure with hashes
- Blob storage verification
- Sample converted data
- Hash integrity checks
- @rljson/db integration

**Shell-based tests (from e2e directory):**

```bash
cd test/e2e
./test-sync.sh
./test-agent-restart.sh
./test-tamper-demo.sh
```

**Node.js-based tests (from project root):**

```bash
# Run from project root to ensure correct paths
node test/e2e/test-integrity-hash.js
node test/e2e/benchmark-state-hash.js
node test/e2e/test-dirty-partitions.js
```

### Running All Tests

```bash
cd test/e2e
./run-all-tests.sh
```

## 📝 Note for Migration

These tests are **not portable** to environments without Docker and MongoDB infrastructure. They are intentionally separated from unit tests to maintain clear boundaries between:

- **Unit tests**: No external dependencies (would be in `test/unit/` if they exist)
- **E2E tests**: Full infrastructure dependencies (this directory)

When migrating this codebase to a new repository or environment, these tests should be:

1. Excluded if the target must be 3rd-party independent
2. Adapted to match the new environment's infrastructure
3. Kept as reference for integration testing patterns

## 🏗️ Infrastructure Requirements

### Docker Compose Services

- `mongoa` - MongoDB A (replica set rsA, port 27017)
- `mongob` - MongoDB B (replica set rsB, port 27018)
- `agenta` - Sync agent for MongoDB A (port 3001)
- `agentb` - Sync agent for MongoDB B (port 3002)
- `hub` - Central relay hub (port 3200)

### Environment Setup

All tests assume:

- Docker and Docker Compose are installed
- Containers are running and healthy
- Network connectivity between containers
- MongoDB replica sets are initialized
- ~552k documents are loaded for performance tests
