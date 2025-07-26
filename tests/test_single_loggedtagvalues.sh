#!/bin/bash

# Test single LoggedTagValues query

echo "📈 Testing Single LoggedTagValues Query"
echo "======================================"

source setenv.sh

# Start server in background with debug logging
echo "Starting server..."
RUST_LOG=debug cargo run -- --debug --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "Running query: SELECT * FROM loggedtagvalues WHERE tag_name = 'HMI_Tag_1' AND timestamp >= '2024-01-01T00:00:00Z' LIMIT 10;"
echo ""

# Run single query
PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable -c "SELECT * FROM loggedtagvalues WHERE tag_name = 'HMI_Tag_1' AND timestamp >= '2024-01-01T00:00:00Z' LIMIT 10;" 2>&1

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "Done."