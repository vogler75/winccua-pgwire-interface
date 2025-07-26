#!/bin/bash

# Test LoggedTagValues queries

echo "📈 Testing LoggedTagValues Queries"
echo "=================================="

if [ -z "$GRAPHQL_HTTP_URL" ]; then
    echo "⚠️  GRAPHQL_HTTP_URL not set, sourcing setenv.sh..."
    source setenv.sh
fi

echo "🌐 GraphQL URL: $GRAPHQL_HTTP_URL"
echo "🎧 Starting server with debug logging..."
echo ""

# Start server in background with debug logging
RUST_LOG=debug cargo run -- --debug --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🧪 Test 1: Basic LoggedTagValues query"
echo "Query: SELECT * FROM loggedtagvalues WHERE tag_name = 'HMI_Tag_1';"
echo ""

PGPASSWORD=password1 timeout 15s psql -h localhost -p 5433 -U username1 --set=sslmode=disable -c "SELECT * FROM loggedtagvalues WHERE tag_name = 'HMI_Tag_1';" 2>&1 &

sleep 8

echo ""
echo "🧪 Test 2: LoggedTagValues with time range"
echo "Query: SELECT * FROM loggedtagvalues WHERE tag_name = 'HMI_Tag_1' AND timestamp >= '2024-01-01T00:00:00Z' AND timestamp <= '2024-12-31T23:59:59Z';"
echo ""

PGPASSWORD=password1 timeout 15s psql -h localhost -p 5433 -U username1 --set=sslmode=disable -c "SELECT * FROM loggedtagvalues WHERE tag_name = 'HMI_Tag_1' AND timestamp >= '2024-01-01T00:00:00Z' AND timestamp <= '2024-12-31T23:59:59Z';" 2>&1 &

sleep 8

echo ""
echo "🧪 Test 3: LoggedTagValues with LIMIT"
echo "Query: SELECT * FROM loggedtagvalues WHERE tag_name = 'HMI_Tag_1' LIMIT 10;"
echo ""

PGPASSWORD=password1 timeout 15s psql -h localhost -p 5433 -U username1 --set=sslmode=disable -c "SELECT * FROM loggedtagvalues WHERE tag_name = 'HMI_Tag_1' LIMIT 10;" 2>&1 &

sleep 8

echo ""
echo "✅ Tests completed. Check server logs for error details."
echo "🛑 Stopping server..."

# Kill the server
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "🔚 Test finished."