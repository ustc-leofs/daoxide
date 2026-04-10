#!/bin/bash
# Quality gate script for daoxide
# This script runs all quality checks that would run in CI
# Usage: ./scripts/ci.sh [--fast] [--verbose]
#   --fast    Skip slow checks (doc, full test matrix)
#   --verbose Show detailed output

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

FAST_MODE=false
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --fast)
            FAST_MODE=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [--fast] [--verbose]"
            echo "  --fast    Skip slow checks (doc, full test matrix)"
            echo "  --verbose Show detailed output"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

failed=0
passed=0

run_check() {
    local name="$1"
    shift
    local cmd="$*"

    if [ "$VERBOSE" = true ]; then
        echo -e "${YELLOW}[RUN]${NC} $name"
        echo "       Command: $cmd"
    fi

    if eval "$cmd"; then
        echo -e "${GREEN}[PASS]${NC} $name"
        ((passed++))
    else
        echo -e "${RED}[FAIL]${NC} $name"
        ((failed++))
    fi
}

echo "=========================================="
echo "  daoxide Quality Gate Runner"
echo "=========================================="
echo ""

echo "Running fmt check..."
run_check "cargo fmt --all -- --check" "cargo fmt --all -- --check"

echo ""
echo "Running clippy with deny warnings..."
run_check "cargo clippy --workspace --all-targets --all-features -- -D warnings" \
    "cargo clippy --workspace --all-targets --all-features -- -D warnings"

echo ""
echo "Running tests (workspace)..."
run_check "cargo test --workspace --all-features" \
    "cargo test --workspace --all-features"

if [ "$FAST_MODE" = false ]; then
    echo ""
    echo "Running doc build..."
    run_check "cargo doc --workspace --no-deps --all-features" \
        "cargo doc --workspace --no-deps --all-features"
fi

echo ""
echo "=========================================="
echo "  Quality Gate Summary"
echo "=========================================="
echo -e "Passed: ${GREEN}$passed${NC}"
echo -e "Failed: ${RED}$failed${NC}"
echo "=========================================="

if [ $failed -gt 0 ]; then
    exit 1
fi

exit 0
