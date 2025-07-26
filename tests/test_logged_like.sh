#!/bin/bash

# Test LIKE pattern support for LoggedTagValues queries

echo "📈 Testing LIKE Patterns for LoggedTagValues"
echo "==========================================="

source setenv.sh

# Start server in background
echo "Starting server..."
RUST_LOG=debug cargo run -- --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🔍 Test 1: Your exact query with LIKE pattern:"
echo "SELECT * FROM loggedtagvalues WHERE tag_name LIKE 'HMI_Tag_%:LoggingTag_1' AND timestamp BETWEEN '2025-07-26T14:00:00Z' AND '2025-07-26T18:00:00Z';"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM loggedtagvalues WHERE tag_name LIKE 'HMI_Tag_%:LoggingTag_1' AND timestamp BETWEEN '2025-07-26T14:00:00Z' AND '2025-07-26T18:00:00Z';" 2>&1

echo ""
echo "🔍 Test 2: Simple prefix pattern for logged values:"
echo "SELECT * FROM loggedtagvalues WHERE tag_name LIKE 'LoggingTag_%' AND timestamp > '2025-07-26T14:00:00Z' LIMIT 5;"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM loggedtagvalues WHERE tag_name LIKE 'LoggingTag_%' AND timestamp > '2025-07-26T14:00:00Z' LIMIT 5;" 2>&1

echo ""
echo "🔍 Test 3: Contains pattern for logged values:"
echo "SELECT * FROM loggedtagvalues WHERE tag_name LIKE '%Logging%' AND timestamp > '2025-07-26T14:00:00Z' LIMIT 5;"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM loggedtagvalues WHERE tag_name LIKE '%Logging%' AND timestamp > '2025-07-26T14:00:00Z' LIMIT 5;" 2>&1

echo ""
echo "🔍 Test 4: Comparison - direct tag name (should still work):"
echo "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' AND timestamp > '2025-07-26T14:00:00Z' LIMIT 5;"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' AND timestamp > '2025-07-26T14:00:00Z' LIMIT 5;" 2>&1

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "💡 Check the debug logs above to see:"
echo "- LIKE pattern conversion for LoggedTagValues"
echo "- Browse results and tag resolution"
echo "- GraphQL query execution with resolved tags"
echo "Done."