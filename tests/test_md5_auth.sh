#!/bin/bash

# Test MD5 authentication implementation

echo "🔐 Testing MD5 Authentication Implementation"
echo "=========================================="

source setenv.sh

# Start server in background with detailed logging
echo "Starting server with MD5 authentication..."
RUST_LOG=debug cargo run -- --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🔍 Test 1: psql connection with known user (should trigger MD5 auth):"
echo ""

# Test with username1/password1
PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT 'MD5 auth test' as message;" 2>&1

echo ""
echo "🔍 Test 2: psql connection with grafana user:"
echo ""

# Test with grafana user (should work if Grafana uses this username)
PGPASSWORD=password1 psql -h localhost -p 5433 -U grafana --set=sslmode=disable \
  -c "SELECT 'Grafana MD5 test' as message;" 2>&1

echo ""
echo "🔍 Test 3: Test with wrong password (should fail):"
echo ""

# Test with wrong password
PGPASSWORD=wrongpassword psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT 'Should fail' as message;" 2>&1

echo ""
echo "🔍 Test 4: Test with unknown user (should fail):"
echo ""

# Test with unknown user
PGPASSWORD=password1 psql -h localhost -p 5433 -U unknownuser --set=sslmode=disable \
  -c "SELECT 'Should fail' as message;" 2>&1

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "💡 What to look for in the logs:"
echo "- 'Sending MD5 password authentication request'"
echo "- 'Received MD5 password response'"
echo "- 'MD5 verification for user: ✅ PASSED' or '❌ FAILED'"
echo "- Salt generation and MD5 hash computation"
echo ""
echo "📝 For Grafana:"
echo "- Use username: 'grafana' and password: 'password1'"
echo "- Or username: 'username1' and password: 'password1'" 
echo "- Make sure to set sslmode=disable in connection settings"
echo "Done."