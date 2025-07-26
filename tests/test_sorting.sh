#!/bin/bash

# Test LoggedTagValues sorting modes

echo "📈 Testing LoggedTagValues Sorting Modes"
echo "======================================="

source setenv.sh

# Start server in background
echo "Starting server..."
RUST_LOG=debug cargo run -- --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🔍 Test 1: Default sorting (should use TIME_ASC):"
echo "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' AND timestamp > '2025-07-26T14:00:00Z' AND timestamp < '2025-07-26T18:00:00Z' LIMIT 5;"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' AND timestamp > '2025-07-26T14:00:00Z' AND timestamp < '2025-07-26T18:00:00Z' LIMIT 5;" 2>&1

echo ""
echo "🔍 Test 2: Explicit ASC sorting (should use TIME_ASC):"
echo "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' AND timestamp > '2025-07-26T14:00:00Z' AND timestamp < '2025-07-26T18:00:00Z' ORDER BY timestamp ASC LIMIT 5;"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' AND timestamp > '2025-07-26T14:00:00Z' AND timestamp < '2025-07-26T18:00:00Z' ORDER BY timestamp ASC LIMIT 5;" 2>&1

echo ""
echo "🔍 Test 3: DESC sorting (should use TIME_DESC):"
echo "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' AND timestamp > '2025-07-26T14:00:00Z' AND timestamp < '2025-07-26T18:00:00Z' ORDER BY timestamp DESC LIMIT 5;"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' AND timestamp > '2025-07-26T14:00:00Z' AND timestamp < '2025-07-26T18:00:00Z' ORDER BY timestamp DESC LIMIT 5;" 2>&1

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "Done."