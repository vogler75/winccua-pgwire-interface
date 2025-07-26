#!/bin/bash

# Test PostgreSQL message parsing error handling

echo "📦 Testing PostgreSQL Message Parsing Error Handling"
echo "==================================================="

source setenv.sh

# Start server in background
echo "Starting server..."
RUST_LOG=warn cargo run -- --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🔍 Test 1: Send malformed PostgreSQL message using netcat:"
echo ""

# Send a malformed message that should trigger parsing error
echo -e "\x01\x02\x03\x04invalid message data" | nc localhost 5433 &
sleep 2

echo ""
echo "🔍 Test 2: Send partially valid message:"
echo ""

# Send a message with wrong length
echo -e "Q\x00\x00\x00\x10incomplete" | nc localhost 5433 &
sleep 2

echo ""
echo "🔍 Test 3: Normal query (should work after parsing errors):"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM tagvalues WHERE tag_name = 'HMI_Tag_1' LIMIT 1;" 2>&1

echo ""
echo "🔍 Test 4: Send completely invalid data:"
echo ""

# Send completely random data
echo -e "\xFF\xFE\xFD\xFC\xFB\xFA\xF9\xF8random_garbage_data_here" | nc localhost 5433 &
sleep 2

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "💡 Expected results in server logs:"
echo "- Detailed PostgreSQL message parsing errors"
echo "- Raw message bytes logged (first 32 bytes)"
echo "- Message type byte and declared length analysis"
echo "- Connection should remain open for valid queries"
echo "- No connection drops on parsing errors"
echo "Done."