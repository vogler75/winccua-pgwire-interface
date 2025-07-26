#!/bin/bash

# Test Parse message validation for unsupported statements

echo "❌ Testing Parse Message Validation"
echo "=================================="

source setenv.sh

# Start server in background
echo "Starting server..."
RUST_LOG=warn cargo run -- --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🔍 Test 1: Try SET statement (should be rejected):"
echo "This will trigger Extended Query Protocol with Parse message"
echo ""

# Many PostgreSQL clients send SET statements during connection setup
# These should be gracefully rejected with proper error messages
PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SET extra_float_digits = 3;" 2>&1

echo ""
echo "🔍 Test 2: Try another unsupported statement:"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SET timezone = 'UTC';" 2>&1

echo ""
echo "🔍 Test 3: Try CREATE statement (should be rejected):"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "CREATE TABLE test (id INT);" 2>&1

echo ""
echo "🔍 Test 4: Try valid SELECT after invalid statements (should work):"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM tagvalues WHERE tag_name = 'HMI_Tag_1' LIMIT 1;" 2>&1

echo ""
echo "🔍 Test 5: Try PREPARE/EXECUTE with valid SELECT (should work):"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "PREPARE test_stmt AS SELECT * FROM tagvalues WHERE tag_name = \$1; EXECUTE test_stmt('HMI_Tag_1');" 2>&1

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "💡 Expected behavior:"
echo "- SET statements should be rejected with clear error messages"
echo "- Other unsupported statements should be rejected"
echo "- Valid SELECT statements should work normally"
echo "- Connection should remain open after rejected Parse messages"
echo "- Server logs should show Parse validation warnings"
echo "Done."