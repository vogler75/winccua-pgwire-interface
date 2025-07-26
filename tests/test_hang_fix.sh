#!/bin/bash

# Test client hang fix for non-SELECT statements

echo "🔧 Testing Client Hang Fix"
echo "========================="

if [ -z "$GRAPHQL_HTTP_URL" ]; then
    echo "⚠️  GRAPHQL_HTTP_URL not set, sourcing setenv.sh..."
    source setenv.sh
fi

echo "🌐 GraphQL URL: $GRAPHQL_HTTP_URL"
echo "🎧 Starting server with hang fix..."
echo ""

# Start server in background with debug logging
RUST_LOG=debug cargo run -- --debug --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🧪 Testing that client doesn't hang after non-SELECT error..."
echo "Expected: Client should receive error but remain responsive for next query"
echo ""

# Test with psql - should not hang after error
{
    echo "Testing with valid credentials and non-SELECT statement..."
    PGPASSWORD=password1 timeout 15s psql -h localhost -p 5433 -U username1 --set=sslmode=disable << 'EOF'
-- This should fail but not hang the client
INSERT INTO test_table VALUES (1, 'test');

-- This should work and prove client is still responsive  
SELECT version();

-- Another test
UPDATE test_table SET name = 'updated';

-- This should also work
SELECT 'Client is responsive' as status;

-- Exit gracefully
\q
EOF
} 2>&1 &

# Wait for test to complete
sleep 8

echo ""
echo "🧪 Testing multiple error scenarios..."
echo ""

# Test multiple errors in sequence
{
    echo "Testing multiple non-SELECT statements..."
    PGPASSWORD=password1 timeout 10s psql -h localhost -p 5433 -U username1 --set=sslmode=disable << 'EOF'
DELETE FROM test_table;
CREATE TABLE test_table (id INT);
ALTER TABLE test_table ADD COLUMN name TEXT;
DROP TABLE test_table;
SELECT 'Still responsive after multiple errors' as test_result;
\q
EOF
} 2>&1 &

# Wait for test to complete
sleep 5

echo ""
echo "✅ Client hang fix tests completed."
echo "🛑 Stopping server..."

# Kill the server
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "🔚 Test finished."
echo ""
echo "If the client remained responsive after errors, the hang fix is working!"