#!/bin/bash

# Test = operator in WHERE clauses

echo "🔍 Testing = Operator Support"
echo "============================="

if [ -z "$GRAPHQL_HTTP_URL" ]; then
    echo "⚠️  GRAPHQL_HTTP_URL not set, sourcing setenv.sh..."
    source setenv.sh
fi

echo "🌐 GraphQL URL: $GRAPHQL_HTTP_URL"
echo "🎧 Starting server..."
echo ""

# Start server in background with debug logging
RUST_LOG=debug cargo run -- --debug --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🧪 Testing = operator with psql..."
echo ""

# Test with = operator
echo "Test 1: Using = operator"
PGPASSWORD=password1 timeout 10s psql -h localhost -p 5433 -U username1 --set=sslmode=disable -c "SELECT * FROM tagvalues WHERE tag_name = 'HMI_Tag_1';" 2>&1 &

sleep 5

echo ""
echo "Test 2: Using IN operator (for comparison)"
PGPASSWORD=password1 timeout 10s psql -h localhost -p 5433 -U username1 --set=sslmode=disable -c "SELECT * FROM tagvalues WHERE tag_name IN ('HMI_Tag_1');" 2>&1 &

sleep 5

echo ""
echo "Test 3: Testing LIKE operator"
PGPASSWORD=password1 timeout 10s psql -h localhost -p 5433 -U username1 --set=sslmode=disable -c "SELECT * FROM tagvalues WHERE tag_name LIKE 'HMI_Tag_%';" 2>&1 &

sleep 5

echo ""
echo "✅ Tests completed."
echo "🛑 Stopping server..."

# Kill the server
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "🔚 Test finished."