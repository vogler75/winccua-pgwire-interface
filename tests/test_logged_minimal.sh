#!/bin/bash

# Test minimal LoggedTagValues queries

echo "📈 Testing Minimal LoggedTagValues Queries"
echo "========================================"

source setenv.sh

# Start server in background
echo "Starting server..."
RUST_LOG=info cargo run -- --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🔍 Test 1: Minimal query without any filters (will fail validation):"
echo "SELECT * FROM loggedtagvalues;"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM loggedtagvalues;" 2>&1

echo ""
echo "🔍 Test 2: Just tag name, no time constraints (will fail validation):"
echo "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1';"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1';" 2>&1

echo ""
echo "🔍 Test 3: With LIMIT but no time constraints (should fail validation):"
echo "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' LIMIT 10;"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' LIMIT 10;" 2>&1

echo ""
echo "🔍 Test 4: With time constraint but no LIMIT:"
echo "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' AND timestamp > '2025-07-26T14:00:00Z';"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' AND timestamp > '2025-07-26T14:00:00Z';" 2>&1

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "💡 Based on these tests, we can determine:"
echo "- If it's a validation issue (missing required parameters)"
echo "- If it's a GraphQL server issue (all queries fail with same error)"
echo "- If it's related to specific parameter combinations"