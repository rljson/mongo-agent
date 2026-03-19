#!/bin/bash
# @license
# Copyright (c) 2025 Rljson
#
# Use of this source code is governed by terms that can be
# found in the LICENSE file in the root of this package.

# Real-Time RLJSON Sync Test with Change Streams
# This script tests automatic RLJSON sync triggered by MongoDB change streams

set -e

echo "🧪 Real-Time RLJSON Sync Test (Change Streams)"
echo "=============================================="
echo ""

# Detect MongoDB connection string
if docker ps | grep -q mongo; then
    echo "📦 Detected MongoDB running in Docker"
    export MONGO_URI="mongodb://localhost:27017/?directConnection=true"
else
    echo "💻 Using local MongoDB"
    export MONGO_URI="mongodb://localhost:27017"
fi

echo "🔗 MongoDB URI: $MONGO_URI"
echo ""

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Navigate to project root (two levels up from test/e2e)
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

echo "📂 Project root: $PROJECT_ROOT"
echo ""
echo "⚠️  IMPORTANT: This test requires MongoDB running in replica set mode"
echo "   If using Docker: docker compose up -d"
echo "   MongoDB container is configured as replica set"
echo ""
echo "▶️  Starting real-time sync test with change streams..."
echo ""

# Run the test from project root
cd "$PROJECT_ROOT"
pnpm exec tsx test/e2e/test-rljson-changestream.ts

echo ""
echo "✅ Real-time sync test with change streams completed!"
echo ""
echo "This test has proven:"
echo "  ✓ MongoDB change streams detect changes in real-time"
echo "  ✓ Changes trigger automatic RLJSON extraction"
echo "  ✓ RLJSON sync propagates changes immediately"
echo "  ✓ Insert, update, and delete operations all work"
echo "  ✓ Complete data consistency maintained"
