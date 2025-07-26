#!/bin/bash

# Test connection parameter logging from various PostgreSQL clients

echo "📋 Testing Connection Parameter Logging"
echo "======================================"

source setenv.sh

# Start server in background with info logging to see parameters
echo "Starting server with parameter logging..."
RUST_LOG=info cargo run -- --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🔍 Test 1: psql connection (standard PostgreSQL client):"
echo ""

# Standard psql connection - should show many parameters
PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT 'psql test' as client_type;" 2>&1

echo ""
echo "🔍 Test 2: psql with application name:"
echo ""

# psql with custom application name
PGPASSWORD=password1 PGAPPNAME="TestApp" psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT 'named app test' as client_type;" 2>&1

echo ""
echo "🔍 Test 3: psql with database name:"
echo ""

# psql with specific database
PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 -d testdb --set=sslmode=disable \
  -c "SELECT 'database test' as client_type;" 2>&1

echo ""
echo "🔍 Test 4: Connection with encoding and timezone:"
echo ""

# Connection with specific encoding and timezone
PGPASSWORD=password1 PGCLIENTENCODING=UTF8 PGTZ=UTC psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT 'encoding test' as client_type;" 2>&1

echo ""
echo "🔍 Test 5: Grafana-style connection simulation:"
echo ""

# Simulate Grafana connection using python (if available)
if command -v python3 &> /dev/null; then
    echo "Using Python to simulate Grafana connection..."
    python3 -c "
import socket
import struct

def send_startup_message():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(('localhost', 5433))
        
        # PostgreSQL startup message
        version = 196608  # PostgreSQL 3.0
        params = b'user\x00grafana\x00database\x00postgres\x00application_name\x00Grafana\x00client_encoding\x00UTF8\x00\x00'
        
        length = 4 + 4 + len(params)
        message = struct.pack('>I', length) + struct.pack('>I', version) + params
        
        sock.send(message)
        
        # Read response 
        response = sock.recv(1024)
        print(f'Server response: {len(response)} bytes received')
        
    except Exception as e:
        print(f'Connection failed: {e}')
    finally:
        sock.close()

send_startup_message()
" 2>&1
else
    echo "Python not available - skipping Grafana simulation"
fi

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "💡 In the server logs above, look for:"
echo "📋 Client connection parameters from [IP]:"
echo "   👤 User: [username]"
echo "   🗄️  Database: [database]"
echo "   📱 Application: [app_name]"
echo "   🔤 Encoding: [encoding]"
echo "   🌍 Timezone: [timezone]"
echo "   📌 [key]: [value] (other parameters)"
echo "📊 Total parameters received: [count]"
echo ""
echo "🔧 For Grafana debugging:"
echo "- Check if 'User' parameter is set correctly"
echo "- Verify 'Database' parameter matches expectation"
echo "- Look for 'Application: Grafana' to confirm it's Grafana"
echo "- Check SSL-related parameters"
echo "Done."