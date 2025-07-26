#!/bin/bash

# Test transaction control statement handling

echo "📋 Testing Transaction Control Statement Handling"
echo "=============================================="

source setenv.sh

# Start server in background with info logging to see ignored statements
echo "Starting server with transaction control handling..."
RUST_LOG=info cargo run -- --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🔍 Test 1: Basic transaction control statements:"
echo ""

# Test BEGIN statement
echo "Testing BEGIN:"
PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "BEGIN;" 2>&1

echo ""
echo "Testing COMMIT:"
PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "COMMIT;" 2>&1

echo ""
echo "Testing ROLLBACK:"
PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "ROLLBACK;" 2>&1

echo ""
echo "🔍 Test 2: Transaction blocks (BEGIN/COMMIT):"
echo ""

# Test transaction block
PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "BEGIN; SELECT 'in transaction' as status; COMMIT;" 2>&1

echo ""
echo "🔍 Test 3: Common utility statements:"
echo ""

# Test SET statements
echo "Testing SET statement:"
PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SET extra_float_digits = 3;" 2>&1

echo ""
echo "Testing SHOW statement:"
PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SHOW server_version;" 2>&1

echo ""
echo "🔍 Test 4: Mixed transaction and data queries:"
echo ""

# Mix transaction control with actual queries
PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable << 'EOF'
BEGIN;
SELECT 'Starting transaction' as message;
SELECT * FROM tagvalues WHERE tag_name = 'HMI_Tag_1' LIMIT 1;
COMMIT;
SELECT 'Transaction complete' as message;
EOF

echo ""
echo "🔍 Test 5: Grafana-style connection with transaction control:"
echo ""

# Simulate Grafana's typical connection pattern
PGPASSWORD=password1 psql -h localhost -p 5433 -U grafana --set=sslmode=disable << 'EOF'
SET extra_float_digits = 3;
BEGIN;
SELECT 1 as connection_test;
COMMIT;
EOF

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "💡 What to look for in the server logs:"
echo "📋 Transaction control statement (acknowledged): BEGIN"
echo "📋 Transaction control statement (acknowledged): COMMIT"
echo "📋 Transaction control statement (acknowledged): ROLLBACK"
echo "🔧 Utility statement (acknowledged): SET extra_float_digits = 3"
echo "🔧 Utility statement (acknowledged): SHOW server_version"
echo ""
echo "✅ Expected behavior:"
echo "- Transaction control statements return proper PostgreSQL CommandComplete responses"
echo "- SET/SHOW statements return appropriate command tags" 
echo "- SELECT queries still work normally within transactions"
echo "- No connection drops or hangs on any statements"
echo "- Proper PostgreSQL protocol compliance with ReadyForQuery messages"
echo "- Grafana and other clients should connect more reliably"
echo "Done."