#!/bin/bash

# Test Grafana-like connection debugging

echo "🔍 Testing Grafana PostgreSQL Connection Debugging"
echo "================================================"

source setenv.sh

# Start server in background with detailed logging
echo "Starting server with detailed logging..."
RUST_LOG=debug cargo run -- --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🔍 Test 1: Standard psql connection (works):"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT 1;" 2>&1

echo ""
echo "🔍 Test 2: Simulate Grafana-style connection with netcat:"
echo "This will help us see what raw protocol messages are exchanged"
echo ""

# Create a simple PostgreSQL startup message manually
# This simulates what Grafana might be sending
python3 -c "
import struct
import sys

# PostgreSQL 3.0 startup message
version = 196608  # 0x00030000
params = b'user\x00grafana\x00database\x00postgres\x00application_name\x00Grafana\x00\x00'

# Calculate total length: 4 (length) + 4 (version) + params
total_length = 4 + 4 + len(params)

# Build message: length + version + params
message = struct.pack('>I', total_length) + struct.pack('>I', version) + params

sys.stdout.buffer.write(message)
" | nc localhost 5433 &

sleep 2

echo ""
echo "🔍 Test 3: Another simulation with different parameters:"
echo ""

python3 -c "
import struct
import sys

# PostgreSQL 3.0 startup message with minimal parameters
version = 196608  # 0x00030000  
params = b'user\x00testuser\x00database\x00testdb\x00\x00'

total_length = 4 + 4 + len(params)
message = struct.pack('>I', total_length) + struct.pack('>I', version) + params

sys.stdout.buffer.write(message)
" | nc localhost 5433 &

sleep 2

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "💡 Check the server logs above for:"
echo "- Startup parameter parsing details"
echo "- Username extraction from startup message"  
echo "- Password authentication request/response"
echo "- Any hex dumps of malformed messages"
echo ""
echo "📝 For Grafana debugging:"
echo "1. Check Grafana's PostgreSQL datasource configuration"
echo "2. Ensure username/password are set correctly"
echo "3. Try with 'sslmode=disable' in connection parameters"
echo "4. Check if Grafana sends different startup parameters"
echo "Done."