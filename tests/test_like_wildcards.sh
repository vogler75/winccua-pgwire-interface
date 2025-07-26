#!/bin/bash

# Test LIKE wildcard patterns for TagValues queries

echo "🔍 Testing LIKE Wildcard Patterns for TagValues"
echo "=============================================="

source setenv.sh

# Start server in background
echo "Starting server..."
RUST_LOG=debug cargo run -- --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🔍 Test 1: Prefix pattern (HMI_Tag_%):"
echo "SELECT * FROM tagvalues WHERE tag_name LIKE 'HMI_Tag_%';"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM tagvalues WHERE tag_name LIKE 'HMI_Tag_%';" 2>&1

echo ""
echo "🔍 Test 2: Suffix pattern (%_Tag_1):"
echo "SELECT * FROM tagvalues WHERE tag_name LIKE '%_Tag_1';"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM tagvalues WHERE tag_name LIKE '%_Tag_1';" 2>&1

echo ""
echo "🔍 Test 3: Contains pattern (%Tag%):"
echo "SELECT * FROM tagvalues WHERE tag_name LIKE '%Tag%';"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM tagvalues WHERE tag_name LIKE '%Tag%';" 2>&1

echo ""
echo "🔍 Test 4: Match all pattern (%):"
echo "SELECT * FROM tagvalues WHERE tag_name LIKE '%' LIMIT 10;"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM tagvalues WHERE tag_name LIKE '%' LIMIT 10;" 2>&1

echo ""
echo "🔍 Test 5: Your specific example (HMI_Tag_%):"
echo "SELECT * FROM tagvalues WHERE tag_name LIKE 'HMI_Tag_%';"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM tagvalues WHERE tag_name LIKE 'HMI_Tag_%';" 2>&1

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "💡 Check the debug logs above to see:"
echo "- How SQL LIKE patterns are converted to GraphQL browse patterns"
echo "- How many tags the browse function returns"
echo "- The final tagValues query results"
echo "Done."