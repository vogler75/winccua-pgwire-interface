#!/bin/bash

# Test authentication fix for error code handling

echo "🔐 Testing Authentication Fix"
echo "============================="

if [ -z "$GRAPHQL_HTTP_URL" ]; then
    echo "⚠️  GRAPHQL_HTTP_URL not set, sourcing setenv.sh..."
    source setenv.sh
fi

echo "🌐 GraphQL URL: $GRAPHQL_HTTP_URL"
echo "🎧 Starting server with detailed authentication logging..."
echo ""

# Start server in background with trace logging for authentication
RUST_LOG=trace cargo run -- --debug --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🧪 Testing authentication with valid credentials..."
echo "Expected: Should now properly handle GraphQL error codes"
echo ""

# Test with valid credentials
{
    echo "username1:password1"
    sleep 3
    echo "SELECT * FROM tagvalues WHERE tag_name = 'HMI_Tag_1';"
    sleep 2
} | nc localhost 5433 &

# Wait for test to complete
sleep 5

echo ""
echo "🧪 Testing authentication with invalid credentials..."
echo "Expected: Should show proper error message with code"
echo ""

# Test with invalid credentials
{
    echo "wronguser:wrongpass"
    sleep 3
} | nc localhost 5433 &

# Wait for test to complete
sleep 3

echo ""
echo "✅ Authentication tests completed. Check server logs above."
echo "🛑 Stopping server..."

# Kill the server
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "🔚 Test finished."
