#!/bin/bash

# Test unknown table error handling without closing connection

echo "❌ Testing Unknown Table Error Handling"
echo "======================================"

source setenv.sh

# Start server in background
echo "Starting server..."
RUST_LOG=info cargo run -- --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🔍 Test 1: Query unknown table (should show error but not close connection):"
echo "SELECT * FROM unknown_table;"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM unknown_table;" 2>&1

echo ""
echo "🔍 Test 2: Another unknown table with complex query:"
echo "SELECT name, value FROM non_existent_table WHERE id = 1;"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT name, value FROM non_existent_table WHERE id = 1;" 2>&1

echo ""
echo "🔍 Test 3: Valid query after unknown table (should work - connection still alive):"
echo "SELECT * FROM tagvalues WHERE tag_name = 'HMI_Tag_1';"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM tagvalues WHERE tag_name = 'HMI_Tag_1';" 2>&1

echo ""
echo "🔍 Test 4: Multiple queries in one session (test connection persistence):"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable <<EOF
SELECT * FROM bad_table;
SELECT * FROM another_bad_table WHERE x = y;
SELECT * FROM tagvalues WHERE tag_name = 'HMI_Tag_1' LIMIT 1;
\q
EOF

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "💡 Expected results:"
echo "- Unknown table errors should be logged with SQL statement details"
echo "- Connection should remain open after unknown table errors"  
echo "- Valid queries should work after unknown table errors"
echo "- Server logs should show available table names"
echo "Done."