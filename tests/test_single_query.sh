#!/bin/bash

# Test single LoggedTagValues query with full debug output

echo "📈 Testing Single LoggedTagValues Query"
echo "====================================="

source setenv.sh

# Start server in background
echo "Starting server..."
RUST_LOG=info cargo run -- --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🔍 Running query with detailed GraphQL output:"
echo "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' AND timestamp > '2025-07-26T14:00:00Z' AND timestamp < '2025-07-26T18:00:00Z' LIMIT 10;"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' AND timestamp > '2025-07-26T14:00:00Z' AND timestamp < '2025-07-26T18:00:00Z' LIMIT 10;" 2>&1

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "Done."