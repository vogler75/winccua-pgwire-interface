#!/bin/bash

# Test PostgreSQL password authentication

echo "🔐 Testing PostgreSQL Password Authentication"
echo "============================================="

if [ -z "$GRAPHQL_HTTP_URL" ]; then
    echo "⚠️  GRAPHQL_HTTP_URL not set, sourcing setenv.sh..."
    source setenv.sh
fi

echo "🌐 GraphQL URL: $GRAPHQL_HTTP_URL"
echo "🎧 Starting server with password authentication..."
echo ""

# Start server in background with debug logging
RUST_LOG=debug cargo run -- --debug --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🧪 Testing psql connection (should prompt for password)..."
echo "Expected: psql should ask for password and authenticate"
echo ""

# Test with psql - should prompt for password
echo "Connecting with user 'username1' - you should be prompted for password"
echo "Try password: 'password1'"
echo ""

timeout 30s psql -h localhost -p 5433 -U username1 --set=sslmode=disable -c "SELECT version();" 2>&1 &

# Wait for test to complete
sleep 10

echo ""
echo "🧪 Testing interactive psql (will timeout after 15 seconds)..."
echo "You should be prompted for password. Try: 'password1'"
echo "Then you can run: SELECT * FROM tagvalues WHERE tag_name = 'HMI_Tag_1';"
echo ""

# Test interactive psql
timeout 15s psql -h localhost -p 5433 -U username1 --set=sslmode=disable 2>&1 &

# Wait for interactive test
sleep 17

echo ""
echo "🧪 Testing with wrong password (should fail)..."
echo ""

# Test with wrong password using PGPASSWORD environment variable
PGPASSWORD=wrongpassword timeout 10s psql -h localhost -p 5433 -U username1 --set=sslmode=disable -c "SELECT version();" 2>&1 &

# Wait for test to complete
sleep 12

echo ""
echo "✅ Password authentication tests completed."
echo "🛑 Stopping server..."

# Kill the server
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "🔚 Test finished."
echo ""
echo "If psql prompted for password, the authentication is working correctly!"