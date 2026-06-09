# Mongo Agent Sync: Cross-Laptop Convergence Guide

## Purpose
This document captures the full context, steps, and troubleshooting process for achieving deterministic, bidirectional MongoDB sync between two laptops (Laptop 1 and Laptop 2) using the @rljson/mongo-agent system.

---

## 1. Objective
- Ensure both MongoDB instances (Laptop 1 and Laptop 2) converge to the same state, with matching dbRoot hashes as proof.
- Achieve robust, automatic, and tamper-evident sync across network boundaries (Wi-Fi, Ethernet).

---

## 2. Architecture Overview
- **Hub**: Central relay server (Fastify/Express, port 3200) for agent registration and relayed sync.
- **Agent**: Fastify HTTP server on each laptop, monitors MongoDB via change streams, syncs via hub.
- **MongoDB**: Each laptop runs a MongoDB replica set (rs0), with local data and change streams.

---

## 3. Key Environment Variables
- `NODE_ID`: Unique agent name (e.g., laptop1, laptop2)
- `PORT`: Agent listen port (e.g., 3001, 3002)
- `MONGO_URI`: MongoDB connection string
- `DB_NAME`: Database name
- `HUB_URL`: Hub server URL (e.g., http://192.168.178.63:3200)
- `PEERS`: Comma-separated peer agent IDs
- `AGENT_URL`: (Critical) The full URL the agent advertises to the hub (e.g., http://192.168.178.64:3002)
- `SYNC_INTERVAL_MS`: Poll interval for sync
- `USE_RLJSON_SYNC`: false (operation-based sync)

---

## 4. Setup & Connection Steps
### 4.1. Firewall & Network
- Open inbound firewall rules for agent and hub ports (TCP 3001/3002, 3200, ICMP for ping).
- Ensure both laptops are on the same Wi-Fi SSID or direct Ethernet link.
- Confirm network reachability with `Test-NetConnection` and `Invoke-WebRequest`.

### 4.2. Agent Registration & Hub Connectivity
- Each agent must register with the hub using its real LAN IP, not localhost.
- Use `AGENT_URL` in `.env` to override auto-detection if needed.
- Verify registration at hub: `/hub/clients` endpoint must show both agents with correct IPs.

### 4.3. Starting the Agent
- Use: `node --env-file=.env --import tsx/esm src/agent-server.ts`
- Confirm logs show correct `selfUrl` and successful registration.

---

## 5. Troubleshooting & Fixes
### 5.1. Root Cause of Sync Failure
- If agent registers with `localhost`, hub relays fail (502/404), sync does not work.
- Fix: Set `AGENT_URL` to the correct LAN IP and port in `.env`.

### 5.2. Common Issues
- Firewall blocks: Open rules for all relevant ports.
- Wrong IP in registration: Always check `/hub/clients` for correct URLs.
- VPN or Ethernet precedence: Use Wi-Fi IP for LAN sync unless direct cable is used.

---

## 6. Bulk Seeding & Convergence
- If collections are missing on one side, perform a bulk seed from the complete side.
- After seeding, trigger sync and re-hash both sides to confirm convergence.

---

## 7. Verification
- Insert a document on either side; it should appear on the other within ~2 seconds.
- Use `_sync-status.mts --summary` to check collection counts and lastSeqSeen.
- Use `/hub/clients` to verify both agents are registered and reachable.
- Final check: dbRoot hash must match on both sides.

---

## 8. Example Commands
- Check agent health: `Invoke-WebRequest http://<agent-ip>:<port>/health`
- Check hub clients: `Invoke-WebRequest http://<hub-ip>:3200/hub/clients`
- Start agent: `node --env-file=.env --import tsx/esm src/agent-server.ts`
- Bulk seed (manual): Use MongoDB tools or scripts to copy missing collections.

---

## 9. References
- See MESSAGE-FOR-LAPTOP2-ETHERNET.txt for detailed troubleshooting history and step-by-step logs.
- See README.architecture.md for system design.

---

## 10. Lessons Learned
- Always verify agent registration IPs.
- Use explicit overrides for network edge cases.
- Monitor logs for registration, relay, and sync errors.
- Deterministic convergence requires both structural and operational parity.

---

*This document should be updated as new issues and solutions are discovered during cross-laptop sync operations.*
