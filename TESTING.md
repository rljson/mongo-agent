# MongoDB Sync Testing Guide

## Start the System

```bash
cd /Users/hermanmertke/dev/mongo-agent
docker compose up -d
```

## Check Service Health

```bash
# Check all services
docker compose ps

# Check agent logs
docker compose logs -f agenta agentb

# Check hub logs
docker compose logs -f hub
```

## Test APIs

```bash
# Agent A health & info
curl http://localhost:3001/health
curl http://localhost:3001/sync/info

# Agent B health & info
curl http://localhost:3002/health
curl http://localhost:3002/sync/info

# Hub health
curl http://localhost:3200/health
```

## Insert Test Data

### Connect to MongoDB A
```bash
docker compose exec mongoa mongosh syncdb
```

Then in mongosh:
```javascript
// Insert a test article
db.articles.insertOne({
  _id: "test-" + Date.now(),
  title: "Test Article from Node A",
  author: "Test User",
  content: "This should sync to Node B",
  createdAt: new Date()
})

// Check articles
db.articles.find().pretty()
db.articles.countDocuments()

// Check sync operations
db.sync_ops.find().sort({seq: -1}).limit(5).pretty()

// Check sync state
db.sync_state.find().pretty()

// Check local state
db.sync_local.findOne({_id: "local"})
```

### Connect to MongoDB B
```bash
docker compose exec mongob mongosh syncdb
```

Then verify the article synced:
```javascript
// Should see the same articles as Node A after a few seconds
db.articles.find().pretty()
db.articles.countDocuments()

// Check received operations
db.sync_ops.find().sort({seq: -1}).limit(5).pretty()
```

## Test Sync with Script

```bash
chmod +x test-sync.sh
./test-sync.sh
```

## Check Sync State via API

```bash
# Agent A's view of Node B
curl http://localhost:3001/sync/state/nodeB | jq

# Agent B's view of Node A  
curl http://localhost:3002/sync/state/nodeA | jq
```

## Bi-Directional Test

```bash
# Insert into Node A
docker compose exec mongoa mongosh syncdb --eval 'db.articles.insertOne({
  _id: "from-a-" + Date.now(),
  title: "From Node A",
  source: "nodeA"
})'

# Wait 2 seconds
sleep 2

# Insert into Node B
docker compose exec mongob mongosh syncdb --eval 'db.articles.insertOne({
  _id: "from-b-" + Date.now(), 
  title: "From Node B",
  source: "nodeB"
})'

# Wait 2 seconds  
sleep 2

# Both should have both documents
docker compose exec mongoa mongosh syncdb --eval 'db.articles.countDocuments()'
docker compose exec mongob mongosh syncdb --eval 'db.articles.countDocuments()'
```

## Debugging

```bash
# Watch logs in real-time
docker compose logs -f

# Check specific service
docker compose logs -f agenta

# Restart a service
docker compose restart agenta

# Check MongoDB logs
docker compose logs mongoa
```

## Cleanup

```bash
# Stop everything
docker compose down

# Stop and remove volumes (deletes all data)
docker compose down -v
```

## Direct MongoDB Commands (One-Liners)

```bash
# Count articles in MongoDB A
docker compose exec mongoa mongosh syncdb --quiet --eval 'db.articles.countDocuments()'

# Count articles in MongoDB B  
docker compose exec mongob mongosh syncdb --quiet --eval 'db.articles.countDocuments()'

# Show last 3 operations in MongoDB A
docker compose exec mongoa mongosh syncdb --quiet --eval 'db.sync_ops.find().sort({seq:-1}).limit(3).toArray()'

# Show sync state in MongoDB A
docker compose exec mongoa mongosh syncdb --quiet --eval 'db.sync_state.find().toArray()'
```
