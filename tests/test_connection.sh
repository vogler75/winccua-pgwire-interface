#!/bin/bash

# Script to test GraphQL connection validation with various scenarios

echo "Testing GraphQL Connection Validation"
echo "======================================"

# Test 1: Invalid URL (should fail)
echo ""
echo "Test 1: Invalid URL"
GRAPHQL_HTTP_URL="http://invalid-url:9999/graphql" cargo run -- --debug --bind-addr 127.0.0.1:5433 2>&1 | head -20

# Test 2: Valid URL format but wrong port (should fail)
echo ""
echo "Test 2: Wrong port" 
GRAPHQL_HTTP_URL="http://localhost:9999/graphql" cargo run -- --debug --bind-addr 127.0.0.1:5434 2>&1 | head -20

# Test 3: Real GraphQL playground (should work if accessible)
echo ""
echo "Test 3: Public GraphQL endpoint"
GRAPHQL_HTTP_URL="https://countries.trevorblades.com/" cargo run -- --debug --bind-addr 127.0.0.1:5435 2>&1 | head -20

echo ""
echo "Tests completed. Check the debug output above for validation behavior."