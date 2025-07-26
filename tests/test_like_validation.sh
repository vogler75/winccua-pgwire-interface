#!/bin/bash

# Test LIKE validation fix for TagValues queries

echo "🔍 Testing LIKE Validation Fix"
echo "=============================="

source setenv.sh

# Start server in background
echo "Starting server..."
RUST_LOG=debug cargo run -- --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🔍 Test 1: Simple LIKE pattern (should work now):"
echo "SELECT * FROM tagvalues WHERE tag_name LIKE 'HMI_Tag_%';"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM tagvalues WHERE tag_name LIKE 'HMI_Tag_%';" 2>&1

echo ""
echo "🔍 Test 2: Query without tag_name filter (should still fail):"
echo "SELECT * FROM tagvalues;"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM tagvalues;" 2>&1

echo ""
echo "🔍 Test 3: Equality filter (should work as before):"
echo "SELECT * FROM tagvalues WHERE tag_name = 'HMI_Tag_1';"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM tagvalues WHERE tag_name = 'HMI_Tag_1';" 2>&1

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "💡 Expected results:"
echo "- Test 1: Should work and show debug logs about pattern conversion"
echo "- Test 2: Should fail with validation error (no tag filter)"  
echo "- Test 3: Should work with direct tag name query"
echo "Done."