#!/bin/bash
# Test script for MongoDB sync in Docker
set -e

echo "=== MongoDB Sync Test Script ==="
echo ""

# Check if services are running
echo "1. Checking services..."
docker compose ps

echo ""
echo "2. Testing Agent A API..."
curl -s http://localhost:3001/health | jq .
curl -s http://localhost:3001/sync/info | jq .

echo ""
echo "3. Testing Agent B API..."
curl -s http://localhost:3002/health | jq .
curl -s http://localhost:3002/sync/info | jq .

echo ""
echo "4. Testing Hub API..."
curl -s http://localhost:3200/health | jq .

echo ""
echo "5. Insert test document into MongoDB A..."
docker compose exec mongoa mongosh syncdb --quiet --eval '
  db.articles.insertOne({
    _id: "test-" + Date.now(),
    title: "Test Article",
    author: "Test Script",
    content: "This document should sync to MongoDB B",
    createdAt: new Date().toISOString()
  })
'

echo ""
echo "6. Wait 3 seconds for sync..."
sleep 3

echo ""
echo "7. Check MongoDB A articles count..."
COUNTA=$(docker compose exec mongoa mongosh syncdb --quiet --eval 'db.articles.countDocuments()' | tail -1)
echo "MongoDB A articles: $COUNTA"

echo ""
echo "8. Check MongoDB B articles count..."
COUNTB=$(docker compose exec mongob mongosh syncdb --quiet --eval 'db.articles.countDocuments()' | tail -1)
echo "MongoDB B articles: $COUNTB"

echo ""
echo "9. Check sync_ops in MongoDB A..."
docker compose exec mongoa mongosh syncdb --quiet --eval '
  db.sync_ops.find({}, {_id:1, origin:1, seq:1, operationType:1}).sort({seq:-1}).limit(3).toArray()
'

echo ""
echo "10. Check sync_ops in MongoDB B..."
docker compose exec mongob mongosh syncdb --quiet --eval '
  db.sync_ops.find({}, {_id:1, origin:1, seq:1, operationType:1}).sort({seq:-1}).limit(3).toArray()
'

echo ""
echo "11. Check Agent A sync state..."
curl -s http://localhost:3001/sync/state/nodeB | jq .

echo ""
echo "12. Check Agent B sync state..."
curl -s http://localhost:3002/sync/state/nodeA | jq .

echo ""
echo "=== Test Complete ==="
