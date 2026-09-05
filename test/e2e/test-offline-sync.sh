#!/bin/bash
# Offline Write Sync Test
# 
# This script demonstrates:
# 1. Agents are running and syncing normally
# 2. Agents go offline (stopped)
# 3. Writes happen to both databases while agents are offline
# 4. Agents come back online
# 5. Changes sync correctly using resume tokens (if within oplog window)
# 6. Both databases end up with all changes from both sides
#
# NOTE: This depends on MongoDB oplog size. Resume tokens work as long as:
# - The offline period is shorter than oplog retention
# - Oplog hasn't been truncated
# - If resume token is invalid, agents will start from "now" and log a warning

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

cd "$(dirname "$0")/.."

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║        Offline Write Sync Test - Resume Token Verification    ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "This test verifies that:"
echo "  • Changes made while agents are offline get synced when they come back"
echo "  • Resume tokens allow agents to pick up where they left off"
echo "  • No data is lost during offline periods"
echo ""

# Generate unique test ID
TEST_ID="offline_test_$(date +%s)_$(openssl rand -hex 4)"
echo "Test ID: $TEST_ID"
echo ""

# ================================================================
# STEP 1: Ensure agents are running initially
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 1: Preparing clean state${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

# Clean up any old test data
docker compose exec -T mongoa mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.deleteMany({testId: {\$regex: '^offline_test'}});" > /dev/null 2>&1 || true
docker compose exec -T mongob mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.deleteMany({testId: {\$regex: '^offline_test'}});" > /dev/null 2>&1 || true

echo -e "${GREEN}✓${NC} Cleaned up old test data"
echo ""

# Start agents if not running
docker compose start agenta agentb > /dev/null 2>&1
sleep 3

AGENT_A_STATUS=$(docker compose ps agenta --format json | jq -r '.[0].State' 2>/dev/null || echo "not running")
AGENT_B_STATUS=$(docker compose ps agentb --format json | jq -r '.[0].State' 2>/dev/null || echo "not running")

echo "Agent A: $AGENT_A_STATUS"
echo "Agent B: $AGENT_B_STATUS"

if [ "$AGENT_A_STATUS" != "running" ] || [ "$AGENT_B_STATUS" != "running" ]; then
  echo -e "${YELLOW}⏳${NC} Starting agents..."
  docker compose up -d agenta agentb
  sleep 5
  echo -e "${GREEN}✓${NC} Agents started"
fi

echo -e "${GREEN}✓${NC} Both agents are running"
echo ""

# Wait a bit for agents to establish sync
sleep 3

# ================================================================
# STEP 2: Take agents offline
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 2: Taking agents OFFLINE${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

echo -e "${YELLOW}⚠${NC}  Stopping sync agents..."
docker compose stop agenta agentb

sleep 2

echo -e "${GREEN}✓${NC} Agents are now OFFLINE"
echo ""

# ================================================================
# STEP 3: Make writes to BOTH databases while offline
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 3: Writing to databases while agents are OFFLINE${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

echo -e "${YELLOW}⏳${NC} Inserting 3 documents on MongoDB A (offline)..."

# Insert 3 docs on MongoDB A
DOC_A1_ID=$(docker compose exec -T mongoa mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
result = db.articles.insertOne({
  testId: '$TEST_ID',
  source: 'MongoDB A',
  docNum: 1,
  offlineWrite: true,
  createdAt: new Date(),
  message: 'Written while agents offline - from A'
});
print(result.insertedId);
" | grep -oE '[a-f0-9]{24}')

DOC_A2_ID=$(docker compose exec -T mongoa mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
result = db.articles.insertOne({
  testId: '$TEST_ID',
  source: 'MongoDB A',
  docNum: 2,
  offlineWrite: true,
  createdAt: new Date(),
  message: 'Written while agents offline - from A'
});
print(result.insertedId);
" | grep -oE '[a-f0-9]{24}')

DOC_A3_ID=$(docker compose exec -T mongoa mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
result = db.articles.insertOne({
  testId: '$TEST_ID',
  source: 'MongoDB A',
  docNum: 3,
  offlineWrite: true,
  createdAt: new Date(),
  message: 'Written while agents offline - from A'
});
print(result.insertedId);
" | grep -oE '[a-f0-9]{24}')

echo "  ✓ Inserted doc A1: $DOC_A1_ID"
echo "  ✓ Inserted doc A2: $DOC_A2_ID"
echo "  ✓ Inserted doc A3: $DOC_A3_ID"
echo ""

echo -e "${YELLOW}⏳${NC} Inserting 3 documents on MongoDB B (offline)..."

# Insert 3 docs on MongoDB B
DOC_B1_ID=$(docker compose exec -T mongob mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
result = db.articles.insertOne({
  testId: '$TEST_ID',
  source: 'MongoDB B',
  docNum: 1,
  offlineWrite: true,
  createdAt: new Date(),
  message: 'Written while agents offline - from B'
});
print(result.insertedId);
" | grep -oE '[a-f0-9]{24}')

DOC_B2_ID=$(docker compose exec -T mongob mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
result = db.articles.insertOne({
  testId: '$TEST_ID',
  source: 'MongoDB B',
  docNum: 2,
  offlineWrite: true,
  createdAt: new Date(),
  message: 'Written while agents offline - from B'
});
print(result.insertedId);
" | grep -oE '[a-f0-9]{24}')

DOC_B3_ID=$(docker compose exec -T mongob mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
result = db.articles.insertOne({
  testId: '$TEST_ID',
  source: 'MongoDB B',
  docNum: 3,
  offlineWrite: true,
  createdAt: new Date(),
  message: 'Written while agents offline - from B'
});
print(result.insertedId);
" | grep -oE '[a-f0-9]{24}')

echo "  ✓ Inserted doc B1: $DOC_B1_ID"
echo "  ✓ Inserted doc B2: $DOC_B2_ID"
echo "  ✓ Inserted doc B3: $DOC_B3_ID"
echo ""

echo -e "${GREEN}✓${NC} 6 documents inserted total (3 on each database) while agents offline"
echo ""

# Check counts before sync
COUNT_A_BEFORE=$(docker compose exec -T mongoa mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({testId: '$TEST_ID'});" | grep -oE '[0-9]+')
COUNT_B_BEFORE=$(docker compose exec -T mongob mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({testId: '$TEST_ID'});" | grep -oE '[0-9]+')

echo "Before sync:"
echo "  MongoDB A has: $COUNT_A_BEFORE test documents"
echo "  MongoDB B has: $COUNT_B_BEFORE test documents"
echo ""

# ================================================================
# STEP 4: Bring agents back ONLINE
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 4: Bringing agents back ONLINE${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

echo -e "${YELLOW}⏳${NC} Starting sync agents..."
docker compose start agenta agentb
sleep 3
echo -e "${GREEN}✓${NC} Agents are now ONLINE"
echo ""

echo -e "${BLUE}ℹ${NC}  Agents will now:"
echo "  • Detect changes from their change streams (using resume tokens)"
echo "  • Create sync operations for offline writes"
echo "  • Exchange operations through the hub"
echo "  • Apply each other's changes"
echo ""

# ================================================================
# STEP 5: Wait for sync to complete
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 5: Waiting for sync to complete${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

WAIT_TIME=15
echo -e "${YELLOW}⏳${NC} Waiting ${WAIT_TIME}s for bidirectional sync..."

for i in $(seq 1 $WAIT_TIME); do
  sleep 1
  printf "  Waiting: %2ds / %ds\r" $i $WAIT_TIME
done
echo ""
echo ""

# ================================================================
# STEP 6: Verify sync completed successfully
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 6: Verifying sync completion${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

# Check if all documents exist on both databases
echo -e "${YELLOW}⏳${NC} Checking if all documents synced to both databases..."
echo ""

# MongoDB A should have all 6 documents
COUNT_A_AFTER=$(docker compose exec -T mongoa mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({testId: '$TEST_ID'});" | grep -oE '[0-9]+')

# MongoDB B should have all 6 documents  
COUNT_B_AFTER=$(docker compose exec -T mongob mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({testId: '$TEST_ID'});" | grep -oE '[0-9]+')

echo "After sync:"
echo "  MongoDB A has: $COUNT_A_AFTER test documents (expected: 6)"
echo "  MongoDB B has: $COUNT_B_AFTER test documents (expected: 6)"
echo ""

# Verify each specific document exists on both sides
echo "Detailed verification:"
echo ""

# Check MongoDB A has all docs
echo "MongoDB A documents:"
HAS_A1_ON_A=$(docker compose exec -T mongoa mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({_id: ObjectId('$DOC_A1_ID')});" | grep -oE '[0-9]+')
HAS_A2_ON_A=$(docker compose exec -T mongoa mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({_id: ObjectId('$DOC_A2_ID')});" | grep -oE '[0-9]+')
HAS_A3_ON_A=$(docker compose exec -T mongoa mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({_id: ObjectId('$DOC_A3_ID')});" | grep -oE '[0-9]+')
HAS_B1_ON_A=$(docker compose exec -T mongoa mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({_id: ObjectId('$DOC_B1_ID')});" | grep -oE '[0-9]+')
HAS_B2_ON_A=$(docker compose exec -T mongoa mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({_id: ObjectId('$DOC_B2_ID')});" | grep -oE '[0-9]+')
HAS_B3_ON_A=$(docker compose exec -T mongoa mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({_id: ObjectId('$DOC_B3_ID')});" | grep -oE '[0-9]+')

echo "  • A1 (from A): $([[ $HAS_A1_ON_A == "1" ]] && echo "✓" || echo "✗")"
echo "  • A2 (from A): $([[ $HAS_A2_ON_A == "1" ]] && echo "✓" || echo "✗")"
echo "  • A3 (from A): $([[ $HAS_A3_ON_A == "1" ]] && echo "✓" || echo "✗")"
echo "  • B1 (from B): $([[ $HAS_B1_ON_A == "1" ]] && echo "✓" || echo "✗")"
echo "  • B2 (from B): $([[ $HAS_B2_ON_A == "1" ]] && echo "✓" || echo "✗")"
echo "  • B3 (from B): $([[ $HAS_B3_ON_A == "1" ]] && echo "✓" || echo "✗")"
echo ""

echo "MongoDB B documents:"
HAS_A1_ON_B=$(docker compose exec -T mongob mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({_id: ObjectId('$DOC_A1_ID')});" | grep -oE '[0-9]+')
HAS_A2_ON_B=$(docker compose exec -T mongob mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({_id: ObjectId('$DOC_A2_ID')});" | grep -oE '[0-9]+')
HAS_A3_ON_B=$(docker compose exec -T mongob mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({_id: ObjectId('$DOC_A3_ID')});" | grep -oE '[0-9]+')
HAS_B1_ON_B=$(docker compose exec -T mongob mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({_id: ObjectId('$DOC_B1_ID')});" | grep -oE '[0-9]+')
HAS_B2_ON_B=$(docker compose exec -T mongob mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({_id: ObjectId('$DOC_B2_ID')});" | grep -oE '[0-9]+')
HAS_B3_ON_B=$(docker compose exec -T mongob mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({_id: ObjectId('$DOC_B3_ID')});" | grep -oE '[0-9]+')

echo "  • A1 (from A): $([[ $HAS_A1_ON_B == "1" ]] && echo "✓" || echo "✗")"
echo "  • A2 (from A): $([[ $HAS_A2_ON_B == "1" ]] && echo "✓" || echo "✗")"
echo "  • A3 (from A): $([[ $HAS_A3_ON_B == "1" ]] && echo "✓" || echo "✗")"
echo "  • B1 (from B): $([[ $HAS_B1_ON_B == "1" ]] && echo "✓" || echo "✗")"
echo "  • B2 (from B): $([[ $HAS_B2_ON_B == "1" ]] && echo "✓" || echo "✗")"
echo "  • B3 (from B): $([[ $HAS_B3_ON_B == "1" ]] && echo "✓" || echo "✗")"
echo ""

# ================================================================
# STEP 7: Cleanup
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 7: Cleanup${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

echo -e "${YELLOW}⏳${NC} Removing test documents..."
docker compose exec -T mongoa mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.deleteMany({testId: '$TEST_ID'});" > /dev/null
docker compose exec -T mongob mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.deleteMany({testId: '$TEST_ID'});" > /dev/null
echo -e "${GREEN}✓${NC} Cleanup complete"
echo ""

# ================================================================
# RESULTS
# ================================================================
echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                       TEST RESULTS                             ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Determine if test passed
PASSED=true

if [ "$COUNT_A_AFTER" != "6" ] || [ "$COUNT_B_AFTER" != "6" ]; then
  PASSED=false
fi

if [ "$HAS_A1_ON_B" != "1" ] || [ "$HAS_A2_ON_B" != "1" ] || [ "$HAS_A3_ON_B" != "1" ]; then
  PASSED=false
fi

if [ "$HAS_B1_ON_A" != "1" ] || [ "$HAS_B2_ON_A" != "1" ] || [ "$HAS_B3_ON_A" != "1" ]; then
  PASSED=false
fi

if [ "$PASSED" = true ]; then
  echo -e "${GREEN}✓ ALL TESTS PASSED! ✨${NC}"
  echo ""
  echo "Verified:"
  echo "  • Changes made while offline were captured by change streams"
  echo "  • Resume tokens allowed agents to pick up from where they left off"
  echo "  • All 6 documents synced successfully to both databases"
  echo "  • No data loss during offline period"
  echo "  • Bidirectional sync works after agents restart"
  echo ""
  exit 0
else
  echo -e "${RED}✗ TEST FAILED${NC}"
  echo ""
  echo "Expected both databases to have 6 documents each."
  echo "Actual: MongoDB A=$COUNT_A_AFTER, MongoDB B=$COUNT_B_AFTER"
  echo ""
  echo "Check agent logs:"
  echo "  docker compose logs agenta"
  echo "  docker compose logs agentb"
  echo ""
  exit 1
fi
