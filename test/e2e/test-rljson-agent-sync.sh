#!/bin/bash
# End-to-End Test: RLJSON Agent-to-Agent Sync
# Verifies that hash-based synchronization works between two agents

set -e

echo "🧪 Starting RLJSON Agent-to-Agent Sync Test..."
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

# Run the test from project root
cd "$PROJECT_ROOT"
pnpm exec tsx test/e2e/test-rljson-agent-sync.ts

echo ""
echo "✅ RLJSON Agent-to-Agent Sync Test complete!"
