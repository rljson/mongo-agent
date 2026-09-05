#!/bin/bash

# Simple shell script to test MongoDB sync between agent A and agent B
# Uses docker compose exec to run commands

set -e

echo "🧪 MongoDB Sync Test Script"
echo "============================"
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

TEST_ID="test_$(date +%s)_$(openssl rand -hex 4)"
WAIT_TIME=4

echo -e "${BLUE}ℹ${NC} Test ID: $TEST_ID"
echo ""

# Test 1: A -> B
echo -e "${BLUE}=== Test 1: Sync from A to B ===${NC}"

echo -e "${YELLOW}⏳${NC} Inserting document on MongoDB A..."
INSERT_RESULT=$(docker compose exec -T mongoa mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
result = db.articles.insertOne({
  testId: '$TEST_ID',
  testCase: 'A->B',
  title: 'Test A->B $TEST_ID',
  content: 'Testing sync from A to B',
  createdBy: 'test-nodeA',
  timestamp: new Date(),
  value: Math.random()
});
print(result.insertedId);
")

DOC_ID_A=$(echo "$INSERT_RESULT" | grep -oE '[a-f0-9]{24}' | head -1)
echo -e "${GREEN}✓${NC} Inserted document with ID: $DOC_ID_A"

echo -e "${YELLOW}⏳${NC} Waiting ${WAIT_TIME}s for sync..."
sleep $WAIT_TIME

echo -e "${YELLOW}⏳${NC} Checking if document exists on MongoDB B..."
CHECK_B=$(docker compose exec -T mongob mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
doc = db.articles.findOne({ testId: '$TEST_ID', testCase: 'A->B' });
if (doc) {
  print('FOUND');
} else {
  print('NOT_FOUND');
}
")

if echo "$CHECK_B" | grep -q "FOUND"; then
  echo -e "${GREEN}✓${NC} Document successfully synced from A to B!"
else
  echo -e "${RED}✗${NC} Document NOT found on MongoDB B!"
  exit 1
fi

echo ""

# Test 2: B -> A
echo -e "${BLUE}=== Test 2: Sync from B to A ===${NC}"

echo -e "${YELLOW}⏳${NC} Inserting document on MongoDB B..."
INSERT_RESULT_B=$(docker compose exec -T mongob mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
result = db.articles.insertOne({
  testId: '$TEST_ID',
  testCase: 'B->A',
  title: 'Test B->A $TEST_ID',
  content: 'Testing sync from B to A',
  createdBy: 'test-nodeB',
  timestamp: new Date(),
  value: Math.random()
});
print(result.insertedId);
")

DOC_ID_B=$(echo "$INSERT_RESULT_B" | grep -oE '[a-f0-9]{24}' | head -1)
echo -e "${GREEN}✓${NC} Inserted document with ID: $DOC_ID_B"

echo -e "${YELLOW}⏳${NC} Waiting ${WAIT_TIME}s for sync..."
sleep $WAIT_TIME

echo -e "${YELLOW}⏳${NC} Checking if document exists on MongoDB A..."
CHECK_A=$(docker compose exec -T mongoa mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
doc = db.articles.findOne({ testId: '$TEST_ID', testCase: 'B->A' });
if (doc) {
  print('FOUND');
} else {
  print('NOT_FOUND');
}
")

if echo "$CHECK_A" | grep -q "FOUND"; then
  echo -e "${GREEN}✓${NC} Document successfully synced from B to A!"
else
  echo -e "${RED}✗${NC} Document NOT found on MongoDB A!"
  exit 1
fi

echo ""

# Cleanup
echo -e "${BLUE}=== Cleanup ===${NC}"
echo -e "${YELLOW}⏳${NC} Removing test documents..."

docker compose exec -T mongoa mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
result = db.articles.deleteMany({ testId: '$TEST_ID' });
print('Deleted ' + result.deletedCount + ' from A');
" > /dev/null

docker compose exec -T mongob mongosh --quiet --eval "
db=db.getSiblingDB('syncdb');
result = db.articles.deleteMany({ testId: '$TEST_ID' });
print('Deleted ' + result.deletedCount + ' from B');
" > /dev/null

echo -e "${GREEN}✓${NC} Cleanup complete"

echo ""
echo "=================================================="
echo -e "${GREEN}✓ ALL TESTS PASSED! ✨${NC}"
echo "=================================================="
echo ""
