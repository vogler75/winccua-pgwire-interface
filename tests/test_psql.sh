#!/bin/bash

# Test PostgreSQL wire protocol compatibility with psql

echo "🐘 Testing PostgreSQL Wire Protocol Compatibility"
echo "================================================="

if [ -z "$GRAPHQL_HTTP_URL" ]; then
    echo "⚠️  GRAPHQL_HTTP_URL not set, sourcing setenv.sh..."
    source setenv.sh
fi

echo "🌐 GraphQL URL: $GRAPHQL_HTTP_URL"
echo "🎧 Starting enhanced PostgreSQL-compatible server..."
echo ""

# Start server in background with debug logging
RUST_LOG=debug cargo run -- --debug --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🧪 Testing psql connection with SSL disabled..."
echo "Expected: Should connect and allow queries"
echo ""

# Test with psql (disable SSL to avoid SSL errors)
timeout 30s psql -h localhost -p 5433 -U username1 --set=sslmode=disable -c "SELECT version();" 2>&1 &

# Wait for test to complete
sleep 10

echo ""
echo "🧪 Testing virtual table query..."
echo ""

# Test TagValues query
timeout 30s psql -h localhost -p 5433 -U operator --set=sslmode=disable -c "SELECT * FROM tagvalues WHERE tag_name = 'TestTag';" 2>&1 &

# Wait for test to complete
sleep 10

echo ""
echo "🧪 Testing with psql interactive mode (will timeout after 10 seconds)..."
echo "You can try typing: SELECT * FROM tagvalues WHERE tag_name = 'TestTag';"
echo ""

# Test interactive psql (with timeout)
timeout 10s psql -h localhost -p 5433 -U operator --set=sslmode=disable 2>&1 &

# Wait for interactive test
sleep 12

echo ""
echo "✅ PostgreSQL wire protocol tests completed."
echo "🛑 Stopping server..."

# Kill the server
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "🔚 Test finished."
echo ""
echo "If psql connected successfully, the PostgreSQL wire protocol is working!"
echo "You should be able to use any PostgreSQL client (DBeaver, pgAdmin, etc.)"
