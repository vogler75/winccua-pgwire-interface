#!/bin/bash

# Debug testing script for the WinCC PostgreSQL wire protocol server

echo "🚀 Starting WinCC PostgreSQL Wire Protocol Server in debug mode..."
echo "================================================="

# Check if GraphQL URL is set
if [ -z "$GRAPHQL_HTTP_URL" ]; then
    echo "⚠️  GRAPHQL_HTTP_URL not set, sourcing setenv.sh..."
    source setenv.sh
fi

echo "🌐 GraphQL URL: $GRAPHQL_HTTP_URL"
echo "🎧 Server will listen on: 127.0.0.1:5433"
echo ""
echo "📋 To test the server:"
echo "  1. Open another terminal"
echo "  2. Connect with: nc localhost 5433"
echo "  3. Send authentication: username:password"
echo "  4. Send query: SELECT * FROM tagvalues WHERE tag_name = 'TestTag';"
echo ""
echo "🐘 Note: DBeaver expects PostgreSQL wire protocol, not simple TCP!"
echo "   DBeaver connections will show detailed protocol detection."
echo ""
echo "Press Ctrl+C to stop the server"
echo "================================================="
echo ""

# Start the server with debug logging
RUST_LOG=debug cargo run -- --debug --bind-addr 127.0.0.1:5433