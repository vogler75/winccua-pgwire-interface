#!/bin/bash

# Test SSL connection handling with PostgreSQL clients

echo "🔒 Testing SSL Connection Handling"
echo "=================================="

if [ -z "$GRAPHQL_HTTP_URL" ]; then
    echo "⚠️  GRAPHQL_HTTP_URL not set, sourcing setenv.sh..."
    source setenv.sh
fi

echo "🚀 Starting server with SSL debug logging..."
echo ""

# Start server in background with enhanced logging
RUST_LOG=trace cargo run -- --debug --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🧪 Testing SSL connection with psql..."
echo "Expected: SSL should be rejected gracefully"
echo ""

# Test with psql (this should trigger SSL negotiation)
timeout 10s psql -h localhost -p 5433 -U testuser -d testdb 2>&1 | head -20 &

# Wait a moment then show what happened
sleep 5

echo ""
echo "🧪 Testing with DBeaver-style connection..."
echo "Expected: PostgreSQL wire protocol detected and proper error sent"
echo ""

# Simulate what DBeaver sends (SSL request followed by startup)
{
    # Send SSL request: length=8, version=80877103
    printf '\x00\x00\x00\x08\x04\xd2\x16\x2f'
    sleep 1
    # Send startup message (simplified)
    printf '\x00\x00\x00\x20\x00\x03\x00\x00user\x00testuser\x00database\x00testdb\x00\x00'
    sleep 1
} | nc localhost 5433 &

sleep 3

echo ""
echo "✅ SSL tests completed. Check server logs above for detailed output."
echo "🛑 Stopping server..."

# Kill the server
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "🔚 Test finished."