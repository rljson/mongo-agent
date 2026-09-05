#!/bin/bash
# Shell script to verify state hashes match between MongoDB A and B

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║       MongoDB State Hash Verification Test          ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

echo -e "${BLUE}ℹ${NC} Running Node.js state hash comparison..."
echo ""

npx tsx test-state-hash.ts

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  echo -e "${GREEN}✓${NC} State hash test completed successfully!"
  exit 0
else
  echo -e "${RED}✗${NC} State hash test failed!"
  exit 1
fi
