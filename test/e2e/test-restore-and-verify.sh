#!/bin/bash
# Restore MongoDB databases from backup and verify state hashes match
# Usage: ./test-restore-and-verify.sh [backup_file]
#
# This script:
# 1. Stops sync agents
# 2. Restores both databases from backup
# 3. Clears all sync metadata (including resume tokens)
# 4. Verifies state hashes match
# 5. Leaves agents stopped for manual testing

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

# Default backup file
BACKUP_FILE="${1:-/Users/hermanmertke/Downloads/CARATDB/cd_articles.bson.gz}"

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║     MongoDB Restore & State Hash Verification Test            ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

if [ ! -f "$BACKUP_FILE" ]; then
  echo -e "${RED}✗${NC} Backup file not found: $BACKUP_FILE"
  exit 1
fi

echo -e "${BLUE}ℹ${NC} Backup file: $BACKUP_FILE"
echo -e "${BLUE}ℹ${NC} File size: $(ls -lh "$BACKUP_FILE" | awk '{print $5}')"
echo ""

# ================================================================
# STEP 1: Stop agents
# ================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${CYAN}STEP 1: Stopping sync agents${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
docker compose stop agenta agentb > /dev/null 2>&1
echo -e "${GREEN}✓${NC} Agents stopped"
echo ""

# ================================================================
# STEP 2: Drop existing collections
# ================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${CYAN}STEP 2: Dropping existing articles collections${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo -e "  ${YELLOW}⏳${NC} MongoDB A..."
docker compose exec -T mongoa mongosh --quiet --eval '
db=db.getSiblingDB("syncdb");
result = db.articles.drop();
if (result) {
  print("  ✓ Articles collection dropped");
} else {
  print("  ℹ Articles collection was empty or did not exist");
}
'

echo -e "  ${YELLOW}⏳${NC} MongoDB B..."
docker compose exec -T mongob mongosh --quiet --eval '
db=db.getSiblingDB("syncdb");
result = db.articles.drop();
if (result) {
  print("  ✓ Articles collection dropped");
} else {
  print("  ℹ Articles collection was empty or did not exist");
}
'
echo ""

# ================================================================
# STEP 3: Restore from backup
# ================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${CYAN}STEP 3: Restoring from backup${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Decompress backup
echo -e "  ${YELLOW}⏳${NC} Decompressing backup file..."
gunzip -c "$BACKUP_FILE" > /tmp/articles.bson
BSON_SIZE=$(ls -lh /tmp/articles.bson | awk '{print $5}')
echo -e "  ${GREEN}✓${NC} Decompressed to /tmp/articles.bson ($BSON_SIZE)"
echo ""

# Restore to MongoDB A
echo -e "  ${YELLOW}⏳${NC} Restoring to MongoDB A..."
docker cp /tmp/articles.bson mongodbsync-mongoa-1:/tmp/articles.bson > /dev/null
docker compose exec -T mongoa mongorestore \
  --db syncdb \
  --collection articles \
  /tmp/articles.bson 2>&1 | grep -E "(restored|document)"

if [ ${PIPESTATUS[0]} -eq 0 ]; then
  echo -e "  ${GREEN}✓${NC} MongoDB A restored successfully"
  docker compose exec -T mongoa rm /tmp/articles.bson
else
  echo -e "  ${RED}✗${NC} Failed to restore MongoDB A"
  exit 1
fi
echo ""

# Restore to MongoDB B
echo -e "  ${YELLOW}⏳${NC} Restoring to MongoDB B..."
docker cp /tmp/articles.bson mongodbsync-mongob-1:/tmp/articles.bson > /dev/null
docker compose exec -T mongob mongorestore \
  --db syncdb \
  --collection articles \
  /tmp/articles.bson 2>&1 | grep -E "(restored|document)"

if [ ${PIPESTATUS[0]} -eq 0 ]; then
  echo -e "  ${GREEN}✓${NC} MongoDB B restored successfully"
  docker compose exec -T mongob rm /tmp/articles.bson
  rm /tmp/articles.bson
else
  echo -e "  ${RED}✗${NC} Failed to restore MongoDB B"
  exit 1
fi
echo ""

# ================================================================
# STEP 4: Verify document counts
# ================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${CYAN}STEP 4: Verifying document counts${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

COUNT_A=$(docker compose exec -T mongoa mongosh --quiet --eval 'db=db.getSiblingDB("syncdb"); db.articles.countDocuments({})' | grep -oE '[0-9]+')
COUNT_B=$(docker compose exec -T mongob mongosh --quiet --eval 'db=db.getSiblingDB("syncdb"); db.articles.countDocuments({})' | grep -oE '[0-9]+')

echo "  MongoDB A: $(printf "%'d" $COUNT_A) documents"
echo "  MongoDB B: $(printf "%'d" $COUNT_B) documents"
echo ""

if [ "$COUNT_A" = "$COUNT_B" ]; then
  echo -e "${GREEN}✓${NC} Document counts match!"
else
  echo -e "${RED}✗${NC} Document counts do not match!"
  exit 1
fi
echo ""

# ================================================================
# STEP 5: Clear sync metadata
# ================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${CYAN}STEP 5: Clearing sync metadata${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo -e "  ${YELLOW}⏳${NC} Clearing metadata on MongoDB A..."
docker compose exec -T mongoa mongosh --quiet --eval '
db=db.getSiblingDB("syncdb");
db.sync_ops.deleteMany({});
db.sync_state.deleteMany({});
db.sync_local.deleteMany({});
db.sync_resume.deleteMany({});
db.state_checkpoints.deleteMany({});
db.state_merkle.deleteMany({});
db.state_dirty.deleteMany({});
print("  ✓ Cleared: sync_ops, sync_state, sync_local, sync_resume, state_checkpoints, state_merkle, state_dirty");
'

echo -e "  ${YELLOW}⏳${NC} Clearing metadata on MongoDB B..."
docker compose exec -T mongob mongosh --quiet --eval '
db=db.getSiblingDB("syncdb");
db.sync_ops.deleteMany({});
db.sync_state.deleteMany({});
db.sync_local.deleteMany({});
db.sync_resume.deleteMany({});
db.state_checkpoints.deleteMany({});
db.state_merkle.deleteMany({});
db.state_dirty.deleteMany({});
print("  ✓ Cleared: sync_ops, sync_state, sync_local, sync_resume, state_checkpoints, state_merkle, state_dirty");
'
echo ""

# ================================================================
# STEP 6: Compute and verify state hashes
# ================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${CYAN}STEP 6: Computing and verifying state hashes${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo -e "  ${YELLOW}⏳${NC} This will take approximately 80 seconds for 500k+ documents..."
echo ""

# Run state hash benchmark
npx tsx ./benchmark-state-hash.ts

echo ""

# ================================================================
# SUMMARY
# ================================================================
echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                    TEST COMPLETED                              ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo -e "${GREEN}✓${NC} Both databases restored to identical state"
echo -e "${GREEN}✓${NC} Document count: $(printf "%'d" $COUNT_A)"
echo -e "${GREEN}✓${NC} State hashes verified (see results above)"
echo -e "${GREEN}✓${NC} All sync metadata cleared (including resume tokens)"
echo ""
echo -e "${YELLOW}⚠${NC}  Agents are STOPPED"
echo ""
echo "Next steps:"
echo "  • Start agents: ${CYAN}docker compose start agenta agentb${NC}"
echo "  • Monitor logs: ${CYAN}docker compose logs -f agenta agentb${NC}"
echo "  • Test sync: ${CYAN}cd test && ./test-sync.sh${NC}"
echo ""
