#!/bin/bash

# Test LoggedTagValues with correct logging tag names

echo "📈 Testing LoggedTagValues Queries"
echo "=================================="

source setenv.sh

# Start server in background
echo "Starting server..."
RUST_LOG=info cargo run -- --bind-addr 127.0.0.1:5433 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "🔍 Test 1: Query with logging tag name (without HMI_Tag prefix)"
echo "Query: SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' AND timestamp > '2025-07-26T14:00:00Z' AND timestamp < '2025-07-26T18:00:00Z' LIMIT 10;"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM loggedtagvalues WHERE tag_name = 'LoggingTag_1' AND timestamp > '2025-07-26T14:00:00Z' AND timestamp < '2025-07-26T18:00:00Z' LIMIT 10;" 2>&1

echo ""
echo "🔍 Test 2: Try with browse to find logging tags"
echo "Query: SELECT * FROM tagvalues WHERE tag_name LIKE 'Logging%';"
echo ""

PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
  -c "SELECT * FROM tagvalues WHERE tag_name LIKE 'Logging%';" 2>&1

echo ""
echo "🔍 Test 3: Try common WinCC logging tag patterns"
echo ""

# Common patterns for WinCC logging tags
for pattern in "Archive%" "Log%" "Trend%" "History%"; do
    echo "Trying pattern: $pattern"
    PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable \
      -c "SELECT * FROM tagvalues WHERE tag_name LIKE '$pattern';" 2>&1
    echo ""
done

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "Done."
echo ""
echo "💡 Tips for LoggedTagValues:"
echo "- Logging tags are different from regular tags in WinCC"
echo "- They need to be configured for historical logging in WinCC"
echo "- Common prefixes: Archive_, Log_, Trend_, History_"
echo "- Check your WinCC configuration for actual logging tag names"