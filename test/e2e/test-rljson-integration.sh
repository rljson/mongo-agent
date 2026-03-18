#!/bin/bash
# @license
# Copyright (c) 2025 Rljson
#
# Use of this source code is governed by terms that can be
# found in the LICENSE file in the root of this package.

# RLJSON Integration E2E Test Runner
# This script runs the RLJSON integration test that demonstrates the complete workflow

set -e

echo "🧪 RLJSON Integration E2E Test"
echo "==============================="
echo ""

# Check if Docker containers are running
if command -v docker &> /dev/null; then
    if ! docker ps | grep -q "mongo-agent-mongoa"; then
        echo "⚠️  Warning: MongoDB Docker container not running."
        echo "    Start it with: docker compose up -d"
        echo ""
    else
        echo "✓ MongoDB Docker container is running"
    fi
else
    echo "⚠️  Docker command not found. Assuming MongoDB is running elsewhere."
fi

# Set MongoDB URI from environment or use default
# MongoDB in docker-compose runs as replica set, use directConnection for testing
export MONGO_URI="${MONGO_URI:-mongodb://localhost:27017/?directConnection=true}"

echo "MongoDB URI: $MONGO_URI"
echo ""

# Run the test
echo "Running RLJSON integration test..."
echo ""

cd "$(dirname "$0")/../.."
npx tsx test/e2e/test-rljson-integration.ts

echo ""
echo "✅ Test completed successfully!"
