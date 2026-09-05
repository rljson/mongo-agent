#!/bin/bash
# @license
# Copyright (c) 2025 Rljson
#
# Use of this source code is governed by terms that can be
# found in the LICENSE file in the root of this package.

# Complete RLJSON Workflow Test Runner
# This script runs the comprehensive end-to-end RLJSON workflow test

set -e

echo "🧪 Complete RLJSON Workflow Test"
echo "================================="
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
echo "▶️  Starting complete RLJSON workflow test..."
echo ""

# Run the test from project root
cd "$PROJECT_ROOT"
pnpm exec tsx test/e2e/test-complete-rljson-workflow.ts

echo ""
echo "✅ Complete RLJSON workflow test finished!"
echo ""
echo "This test has proven that the entire RLJSON implementation works:"
echo "  ✓ Hash chain creation"
echo "  ✓ Blob storage"
echo "  ✓ RLJSON protocol sync"
echo "  ✓ Data integrity verification"
echo "  ✓ End-to-end agent synchronization"
