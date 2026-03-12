# E2E Tests for MongoDB Sync

Diese E2E-Tests wurden aus dem alten Repo kopiert und testen die neue TypeScript-Implementation.

## Prerequisites

```bash
# System muss laufen
docker compose up -d

# Warten bis MongoDB Replika-Sets initialisiert sind
docker compose logs mongoa_init mongob_init
```

## Load Test Data

Die E2E-Tests benötigen Daten in der Datenbank. Du hast zwei Optionen:

### Option 1: Schnell Test-Daten generieren (empfohlen)

```bash
cd test/e2e

# Generiert 100 Test-Artikel (Standard)
./seed-testdata.sh

# Oder wähle eine andere Anzahl
./seed-testdata.sh 500
```

### Option 2: Fixtures aus altem Repo laden (1000 Artikel)

```bash
cd test/e2e

# Lädt articles-1000.json aus dem alten Repo
./load-fixtures.js

# Oder gib einen spezifischen Pfad an
FIXTURE_PATH=/path/to/articles.json ./load-fixtures.js
```

## Run Tests

### Integration Tests (New TypeScript Implementation)

Diese Tests validieren die neue TypeScript Implementation:

```bash
cd test/e2e

# Einzelne Integration Tests
node test-agent-apis.js          # Agent Server APIs (Fastify routes)
node test-changestream-sync.js   # Change Stream Monitoring & Bidirectional Sync
node test-hub-relay.js           # Hub Relay & syncOriginFromHub()
./test-sync.sh                   # Shell-basierter Sync Test

# Alle Integration Tests auf einmal
./run-integration-tests.sh
```

**Was diese Tests validieren:**
- ✅ Agent Server Fastify APIs (GET /health, GET /sync/info, POST /sync/pull, GET /sync/state/:origin)
- ✅ Hub Relay Funktionalität (Agent → Hub → Agent sync)
- ✅ Change Stream Monitoring (insert, update, delete operations werden erfasst)
- ✅ Bidirektionaler Sync (A→B und B→A)
- ✅ Hash Chain Integrity (prevHash, chainHash validation)
- ✅ Sync State Tracking (lastSeqPulled, lastSeqApplied)
- ✅ Docker Container Integration

### Unit Tests (Hashing Functions)

Diese Tests prüfen nur die Hashing-Funktionen (nicht die Agent-Server Implementation):

```bash
cd test/e2e

# Hashing Function Tests
node test-integrity-hash.js      # Integrity Hash Tests
node test-state-hash.js          # State Hash Tests
node test-dirty-partitions.js    # Dirty Partition Tests

# Alte E2E Tests aus dem ursprünglichen Repo
./test-tamper-demo.sh           # Tamper Detection Demo
./test-agent-restart.sh         # Agent Restart Test
```

## Environment Variables

Die Tests verwenden diese Standard-URIs:
- MongoDB A: `mongodb://localhost:27017/syncdb?replicaSet=rsA`
- MongoDB B: `mongodb://localhost:27018/syncdb?replicaSet=rsB`

Anpassen mit:
```bash
export MONGO_A_URI="mongodb://localhost:27017/syncdb?replicaSet=rsA"
export MONGO_B_URI="mongodb://localhost:27018/syncdb?replicaSet=rsB"
```

## Troubleshooting

### "Error: connect ECONNREFUSED"
```bash
# Check ob MongoDB läuft
docker compose ps

# Restart falls nötig
docker compose restart mongoa mongob
```

### "MongoServerError: Replica set not initialized"
```bash
# Warten auf Replica Set Init
docker compose logs mongoa_init
docker compose logs mongob_init

# Manuell initialisieren falls nötig
docker compose restart mongoa_init mongob_init
```

### Tests finden keine Daten
```bash
# Check agent logs
docker compose logs -f agenta agentb

# Verify agents sind verbunden
curl http://localhost:3001/sync/info
curl http://localhost:3002/sync/info
```

## Cleanup

```bash
# MongoDB Daten löschen
docker compose down -v

# Container neu starten
docker compose up -d
```
