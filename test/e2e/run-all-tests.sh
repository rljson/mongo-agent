#!/bin/bash
# Run All Tests Suite

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASSED=0
FAILED=0

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║          Running All MongoDB Sync Tests             ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "ℹ  Running shell-based tests only"
echo ""

# Test 1: Basic Sync
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Test 1/1: Basic Bidirectional Sync${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

if ./test-sync.sh; then
  echo -e "${GREEN}✓ Test 1 PASSED${NC}"
  ((PASSED++))
else
  echo -e "${RED}✗ Test 1 FAILED${NC}"
  ((FAILED++))
fi
echo ""

# Summary
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║                  TEST SUMMARY                        ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo -e "  Total tests run: 1"
echo -e "  ${GREEN}Passed: $PASSED${NC}"
echo -e "  ${RED}Failed: $FAILED${NC}"
echo ""
echo -e "${YELLOW}ℹ  Note:${NC} Node.js-based tests skipped (run them manually from project root)"
echo "  - npx tsx test/e2e/benchmark-state-hash.ts"
echo "  - npx tsx test/e2e/test-integrity-hash.ts"
echo "  - npx tsx test/e2e/test-dirty-partitions.ts"
echo ""

if [ $FAILED -eq 0 ]; then
  echo -e "${GREEN}✓ ALL TESTS PASSED! ✨${NC}"
  echo ""
  exit 0
else
  echo -e "${RED}✗ SOME TESTS FAILED${NC}"
  echo ""
  exit 1
fi
