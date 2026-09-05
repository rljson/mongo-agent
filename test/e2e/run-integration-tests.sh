#!/bin/bash
# Run all integration tests for the new TypeScript implementation
set -e

echo ""
echo "═══════════════════════════════════════════════════════════════════════"
echo "  MongoDB Sync Integration Test Suite"
echo "═══════════════════════════════════════════════════════════════════════"
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

FAILED_TESTS=()
PASSED_TESTS=()

run_test() {
  local test_name="$1"
  local test_cmd="$2"

  echo -e "${BLUE}ℹ${NC} Running: ${test_name}"
  echo "───────────────────────────────────────────────────────────────────────"

  if eval "$test_cmd"; then
    echo -e "${GREEN}✓${NC} PASSED: ${test_name}"
    PASSED_TESTS+=("$test_name")
  else
    echo -e "${RED}✗${NC} FAILED: ${test_name}"
    FAILED_TESTS+=("$test_name")
  fi

  echo ""
}

# Check if Docker Compose is running
echo -e "${BLUE}ℹ${NC} Checking if services are running..."
if ! docker compose ps | grep -q "Up"; then
  echo -e "${RED}✗${NC} Docker Compose services are not running!"
  echo -e "${YELLOW}💡${NC} Start services with: docker compose up -d"
  exit 1
fi
echo -e "${GREEN}✓${NC} Services are running"
echo ""

# Integration Tests (test the new implementation)
echo "═══════════════════════════════════════════════════════════════════════"
echo "  Integration Tests (New TypeScript Implementation)"
echo "═══════════════════════════════════════════════════════════════════════"
echo ""

run_test "Agent Server APIs" "npx tsx test-agent-apis.ts"
run_test "Change Stream & Sync" "npx tsx test-changestream-sync.ts"
run_test "Hub Relay & syncOriginFromHub" "npx tsx test-hub-relay.ts"
run_test "Bidirectional Sync (Shell)" "./test-sync.sh"

# Results
echo ""
echo "═══════════════════════════════════════════════════════════════════════"
echo "  Test Results"
echo "═══════════════════════════════════════════════════════════════════════"
echo ""

TOTAL=$((${#PASSED_TESTS[@]} + ${#FAILED_TESTS[@]}))

echo -e "${GREEN}✓${NC} Passed: ${#PASSED_TESTS[@]}/$TOTAL"
for test in "${PASSED_TESTS[@]}"; do
  echo -e "  ${GREEN}✓${NC} $test"
done

if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
  echo ""
  echo -e "${RED}✗${NC} Failed: ${#FAILED_TESTS[@]}/$TOTAL"
  for test in "${FAILED_TESTS[@]}"; do
    echo -e "  ${RED}✗${NC} $test"
  done
  echo ""
  exit 1
else
  echo ""
  echo -e "${GREEN}🎉 All tests passed!${NC}"
  echo ""
  exit 0
fi
