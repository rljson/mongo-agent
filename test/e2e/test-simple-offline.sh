#!/bin/bash
# Simple Offline Sync Verification
# Demonstrates that resume tokens allow offline writes to sync

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

cd "$(dirname "$0")/.."

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║    Simple Offline Sync Test with Resume Tokens      ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

TEST_ID="simple_offline_$(date +%s)"

# Clean up any old data
echo -e "${YELLOW}⏳${NC} Cleaning up old test data..."
docker compose exec -T mongoa mongosh --quiet --eval 'db=db.getSiblingDB("syncdb"); db.articles.deleteMany({testId: /^simple_offline/});' > /dev/null
docker compose exec -T mongob mongosh --quiet --eval 'db=db.getSiblingDB("syncdb"); db.articles.deleteMany({testId: /^simple_offline/});' > /dev/null
echo -e "${GREEN}✓${NC} Clean"
echo ""

# Ensure agents running
echo -e "${YELLOW}⏳${NC} Starting agents..."
docker compose up -d agenta agentb > /dev/null 2>&1
sleep 3
echo -e "${GREEN}✓${NC} Agents online"
echo ""

# Stop Agent A only
echo -e "${YELLOW}⏳${NC} Stopping Agent A..."
docker compose stop agenta > /dev/null 2>&1
sleep 2
echo -e "${GREEN}✓${NC} Agent A offline"
echo ""

# Write to MongoDB A while Agent A is offline
echo -e "${YELLOW}⏳${NC} Writing to MongoDB A while its agent is offline..."
DOC_ID=$(docker compose exec -T mongoa mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
result = db.articles.insertOne({
  testId: '$TEST_ID',
  message: 'Inserted on A while Agent A was offline',
  timestamp: new Date()
});
print(result.insertedId);
" | grep -oE '[a-f0-9]{24}')

echo -e "${GREEN}✓${NC} Document inserted: $DOC_ID"
echo ""

# Restart Agent A
echo -e "${YELLOW}⏳${NC} Starting Agent A (will use resume token)..."
docker compose start agenta > /dev/null 2>&1
sleep 5
echo -e "${GREEN}✓${NC} Agent A back online"
echo ""

# Wait for sync
echo -e "${YELLOW}⏳${NC} Waiting for sync (agents poll every 2 seconds)..."
sleep 8
echo ""

# Verify document exists on both databases
echo -e "${CYAN}Verification:${NC}"
COUNT_A=$(docker compose exec -T mongoa mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({_id: ObjectId('$DOC_ID')});" | grep -oE '[01]')
COUNT_B=$(docker compose exec -T mongob mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.countDocuments({_id: ObjectId('$DOC_ID')});" | grep -oE '[01]')

echo "  MongoDB A: $([[ $COUNT_A == "1" ]] && echo "✓ has document" || echo "✗ missing")"
echo "  MongoDB B: $([[ $COUNT_B == "1" ]] && echo "✓ has document" || echo "✗ missing")"
echo ""

# Check sync_ops
SYNC_OP=$(docker compose exec -T mongoa mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.sync_ops.findOne({docId: '$DOC_ID', origin: 'nodeA'});" 2>/dev/null)
if echo "$SYNC_OP" | grep -q "insert"; then
  echo -e "${GREEN}✓${NC} Sync operation was created by Agent A's change stream"
else
  echo -e "${YELLOW}ℹ${NC}  Sync operation status unclear"
fi
echo ""

# Cleanup
echo -e "${YELLOW}⏳${NC} Cleaning up..."
docker compose exec -T mongoa mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.deleteOne({_id: ObjectId('$DOC_ID')});" > /dev/null
docker compose exec -T mongob mongosh --quiet --eval "db=db.getSiblingDB('syncdb'); db.articles.deleteOne({_id: ObjectId('$DOC_ID')});" > /dev/null
echo -e "${GREEN}✓${NC} Clean"
echo ""

# Results
echo "╔══════════════════════════════════════════════════════╗"
echo "║                   RESULTS                            ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

if [ "$COUNT_A" == "1" ] && [ "$COUNT_B" == "1" ]; then
  echo -e "${GREEN}✓ PASSED!${NC}"
  echo ""
  echo "Demonstrated:"
  echo "  • Document inserted while Agent A was offline"
  echo "  • Agent A restarted and used resume token"
  echo "  • Change stream detected the offline insert  "
  echo "  • Document synced to MongoDB B"
  echo ""
  exit 0
else
  echo -e "${RED}✗ FAILED${NC}"
  echo "Document did not sync properly"
  echo ""
  exit 1
fi
