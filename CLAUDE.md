# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

WinCC UA PostgreSQL Wire Protocol Server - A Rust-based PostgreSQL wire protocol server that translates SQL queries to GraphQL requests for WinCC Unified Architecture (industrial automation system). This allows standard SQL tools to query industrial data without knowing GraphQL.

## Common Development Commands

### Build and Run
```bash
# Build release version
cargo build --release

# Run server (default port 5432)
cargo run -- --graphql-url http://your-wincc-server:4000/graphql --bind-addr 127.0.0.1:5432

# Run with debug logging
RUST_LOG=debug cargo run -- --graphql-url http://your-wincc-server:4000/graphql --debug --bind-addr 127.0.0.1:5433

# Run with TLS encryption enabled
cargo run -- --graphql-url http://your-wincc-server:4000/graphql --bind-addr 127.0.0.1:5432 \
  --tls-enabled --tls-cert server.crt --tls-key server.key

# Run with TLS and client certificate verification
cargo run -- --graphql-url http://your-wincc-server:4000/graphql --bind-addr 127.0.0.1:5432 \
  --tls-enabled --tls-cert server.crt --tls-key server.key \
  --tls-ca-cert ca.crt --tls-require-client-cert
```

### Testing
```bash
# Run integration tests (from /tests/ directory)
./test_psql.sh
./test_basic_queries.sh
python test_datagrip.py
```

### Environment Setup
```bash
# GraphQL endpoint is now provided via command line argument --graphql-url
# No environment variables are required
```

## Architecture

The server acts as a translation layer with DataFusion integration:
1. Receives SQL queries via PostgreSQL wire protocol
2. Parses SQL using DataFusion's sqlparser (unified with DataFusion execution)
3. Routes queries based on complexity:
   - Simple queries → Direct GraphQL translation
   - Complex queries → DataFusion in-memory processing
4. Executes GraphQL queries against WinCC backend
5. For complex queries: loads data into DataFusion Arrow tables and executes SQL
6. Formats results as PostgreSQL responses

### Key Components

**Protocol Layer** (`/src/pg_protocol/`):
- `connection_handler.rs` - Manages client connections and protocol state machine, including TLS negotiation
- `message_handler.rs` - Parses incoming PostgreSQL protocol messages
- `startup.rs` - Handles connection startup and authentication (supports both plain and TLS streams)
- `authentication.rs` - Implements MD5 and SCRAM-SHA-256 auth methods

**TLS Support** (`/src/tls.rs`):
- TLS certificate loading and server configuration
- Client certificate verification (optional)
- Self-signed certificate generation utilities

**Query Translation** (`/src/query_handler/`):
- Each virtual table has its own handler (e.g., `tag_values_handler.rs`)
- `filter.rs` - Translates SQL WHERE clauses to GraphQL filters
- Handlers construct GraphQL queries based on SQL SELECT statements

**SQL Processing**:
- `sql_handler.rs` - Routes SQL queries to appropriate handlers
- `datafusion_handler.rs` - Uses DataFusion for complex SQL operations (GROUP BY, JOINs)
- `tables.rs` - Defines virtual table schemas

### Virtual Tables

- `tagvalues` - Current values from PLCs/tags
- `loggedtagvalues` - Historical tag data with timestamp filtering
- `activealarms` - Currently active alarms
- `loggedalarms` - Historical alarm data
- `tag_list` - Browse available tags in the system

### Query Flow

1. PostgreSQL client connects and authenticates
2. Client sends SQL query
3. Server identifies target virtual table
4. SQL is translated to GraphQL query
5. GraphQL query executed against WinCC backend
6. Results formatted as PostgreSQL rows
7. For complex queries, DataFusion processes results in-memory

## Key Implementation Details

- Uses `pgwire` crate for PostgreSQL protocol implementation
- **SQL parsing uses DataFusion's sqlparser** (same parser used for DataFusion execution)
- All GraphQL communication goes through `src/graphql/client.rs`
- DataFusion integration via `src/datafusion_handler.rs` for complex queries
- Debug logging uses emoji indicators (🚀 startup, 📨 incoming, 📤 outgoing, etc.)
- Supports Extended Query Protocol for prepared statements
- Virtual tables defined in `create_table_function()` in `tables.rs`
- Column type mapping handled in response formatting (e.g., PostgreSQL OIDs)

## Important Notes

- **TLS/SSL support implemented** - Use `--tls-enabled` with certificate files to enable encryption
- GraphQL endpoint must be provided via --graphql-url command line argument
- All timestamp comparisons use ISO 8601 format
- LIKE patterns support % and _ wildcards
- Authentication credentials passed to GraphQL backend via headers