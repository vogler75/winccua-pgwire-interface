#!/bin/bash

# Test TagValues query functionality

echo "🧪 Testing TagValues Query Implementation"
echo "========================================"

if [ -z "$GRAPHQL_HTTP_URL" ]; then
    echo "⚠️  GRAPHQL_HTTP_URL not set, sourcing setenv.sh..."
    source setenv.sh
fi

echo "🌐 GraphQL URL: $GRAPHQL_HTTP_URL"
echo "🎧 Starting server on port 5433..."
echo ""

# Start server in background
RUST_LOG=debug cargo run -- --debug --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🧪 Testing TagValues query..."
echo "Expected: Server should parse SQL query and call GraphQL"
echo ""

# Test TagValues query with proper SQL syntax
{
    echo "operator:secret123"
    sleep 2
    echo "SELECT * FROM tagvalues WHERE tag_name = 'TestTag';"
    sleep 2
} | nc localhost 5433 &

# Wait for test to complete
sleep 5

echo ""
echo "🧪 Testing TagValues query with IN clause..."
echo ""

# Test TagValues query with IN clause
{
    echo "operator:secret123"
    sleep 2
    echo "SELECT tag_name, numeric_value FROM tagvalues WHERE tag_name IN ('TestTag', 'AnotherTag');"
    sleep 2
} | nc localhost 5433 &

# Wait for test to complete
sleep 5

echo ""
echo "✅ TagValues tests completed. Check server logs above for detailed output."
echo "🛑 Stopping server..."

# Kill the server
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "🔚 Test finished."