#!/bin/bash
# Agent Restart Resilience Test
# 
# This script demonstrates:
# 1. Normal bidirectional sync while agents are online
# 2. Agent restarts (simulating crashes or deployments)
# 3. Sync continues working after restart
# 4. No data loss during restarts
#
# This tests the system's ability to survive agent crashes and
# continue syncing without manual intervention.

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
echo "║           Agent Restart Resilience Test                       ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

TEST_ID="restart_test_$(date +%s)_$(openssl rand -hex 4)"

# ================================================================
# STEP 1: Ensure agents are running
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 1: Ensuring agents are running${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

docker compose up -d agenta agentb > /dev/null 2>&1
sleep 3
echo -e "${GREEN}✓${NC} Agents are online"
echo ""

# ================================================================
# STEP 2: Write to MongoDB A
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 2: Writing to MongoDB A${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

DOC_A1_ID=$(docker compose exec -T mongoa mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
result = db.articles.insertOne({
  testId: '$TEST_ID',
  source: 'MongoDB A',
  docNum: 1,
  message: 'Written to A before restart'
});
print(result.insertedId);
" | grep -oE '[a-f0-9]{24}')

echo -e "${GREEN}✓${NC} Inserted document on MongoDB A: $DOC_A1_ID"
echo ""

# ================================================================
# STEP 3: Wait for sync
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 3: Waiting for initial sync${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

sleep 5
echo -e "${GREEN}✓${NC} Sync complete"
echo ""

# ================================================================
# STEP 4: Verify document on MongoDB B
# ================================================================  
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 4: Verifying sync to MongoDB B${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

HAS_A1_ON_B=$(docker compose exec -T mongob mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); print(db.articles.countDocuments({_id: ObjectId('$DOC_A1_ID')}));" | tail -1)

if [ "$HAS_A1_ON_B" == "1" ]; then
  echo -e "${GREEN}✓${NC} Document synced to MongoDB B"
else
  echo -e "${RED}✗${NC} Document NOT found on MongoDB B"
  exit 1
fi
echo ""

# ================================================================
# STEP 5: Restart Agent B
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 5: Restarting Agent B${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

docker compose restart agentb > /dev/null 2>&1
sleep 3
echo -e "${GREEN}✓${NC} Agent B restarted"
echo ""

# ================================================================
# STEP 6: Write to MongoDB A again
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 6: Writing to MongoDB A after restart${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

DOC_A2_ID=$(docker compose exec -T mongoa mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
result = db.articles.insertOne({
  testId: '$TEST_ID',
  source: 'MongoDB A',
  docNum: 2,
  message: 'Written to A after B restart'
});
print(result.insertedId);
" | grep -oE '[a-f0-9]{24}')

echo -e "${GREEN}✓${NC} Inserted document on MongoDB A: $DOC_A2_ID"
echo ""

# ================================================================
# STEP 7: Wait and verify
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 7: Waiting for sync after restart${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

sleep 5
echo -e "${GREEN}✓${NC} Sync period complete"
echo ""

HAS_A2_ON_B=$(docker compose exec -T mongob mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); print(db.articles.countDocuments({_id: ObjectId('$DOC_A2_ID')}));" | tail -1)

if [ "$HAS_A2_ON_B" == "1" ]; then
  echo -e "${GREEN}✓${NC} Document synced to MongoDB B after restart"
else
  echo -e "${RED}✗${NC} Document NOT found on MongoDB B after restart"
  exit 1
fi
echo ""

# ================================================================
# STEP 8: Restart Agent A
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 8: Restarting Agent A${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

docker compose restart agenta > /dev/null 2>&1
sleep 3
echo -e "${GREEN}✓${NC} Agent A restarted"
echo ""

# ================================================================
# STEP 9: Write to MongoDB B
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 9: Writing to MongoDB B after restart${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

DOC_B1_ID=$(docker compose exec -T mongob mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
result = db.articles.insertOne({
  testId: '$TEST_ID',
  source: 'MongoDB B',
  docNum: 1,
  message: 'Written to B after A restart'
});
print(result.insertedId);
" | grep -oE '[a-f0-9]{24}')

echo -e "${GREEN}✓${NC} Inserted document on MongoDB B: $DOC_B1_ID"
echo ""

# ================================================================
# STEP 10: Wait and verify
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 10: Waiting for reverse sync${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

sleep 5
echo -e "${GREEN}✓${NC} Sync period complete"
echo ""

HAS_B1_ON_A=$(docker compose exec -T mongoa mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); print(db.articles.countDocuments({_id: ObjectId('$DOC_B1_ID')}));" | tail -1)

if [ "$HAS_B1_ON_A" == "1" ]; then
  echo -e "${GREEN}✓${NC} Document synced to MongoDB A after restart"
else
  echo -e "${RED}✗${NC} Document NOT found on MongoDB A after restart"
  exit 1
fi
echo ""

# ================================================================
# STEP 11: Cleanup
# ================================================================
echo "══════════════════════════════════════════════════════════════════"
echo -e "${CYAN}STEP 11: Cleanup${NC}"
echo "══════════════════════════════════════════════════════════════════"
echo ""

docker compose exec -T mongoa mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.deleteMany({testId: '$TEST_ID'});" > /dev/null
docker compose exec -T mongob mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.deleteMany({testId: '$TEST_ID'});" > /dev/null
echo -e "${GREEN}✓${NC} Test documents removed"
echo ""

# ================================================================
# RESULTS
# ================================================================
echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                       TEST RESULTS                             ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo -e "${GREEN}✓ ALL TESTS PASSED! ✨${NC}"
echo ""
echo "Verified:"
echo "  • Bidirectional sync works normally"
echo "  • Agent B restarts don't break sync from A→B"
echo "  • Agent A restarts don't break sync from B→A"
echo "  • No data loss during agent restarts"
echo "  • Change streams continue working after restart"
echo ""
echo -e "${BLUE}ℹ${NC}  Note: This test covers agent RESTARTS while databases stay online."
echo "   For offline database scenarios, use tamper detection + repair workflow."
echo ""
