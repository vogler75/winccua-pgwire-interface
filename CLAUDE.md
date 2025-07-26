# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a PostgreSQL wire protocol server implemented in Rust that acts as a proxy to a WinCC UA GraphQL backend. It allows SQL clients to query industrial automation data using familiar SQL syntax.

## Repository Status

✅ **Project Structure Complete**: Core modules and dependencies implemented
✅ **GraphQL Integration**: Client with authentication and query methods  
✅ **Authentication**: Username/password flow with session management
✅ **SQL Parsing**: Query parser with support for SELECT statements and filters
✅ **Simple Server**: Basic TCP server for testing and development
🔄 **Virtual Tables**: Partial implementation, needs full SQL-to-GraphQL translation
📋 **PostgreSQL Wire Protocol**: Planned upgrade from simple TCP to full pgwire

## Key Components

- `src/main.rs` - CLI application entry point
- `src/auth.rs` - Session management and authentication
- `src/graphql/` - GraphQL client and type definitions
- `src/simple_server.rs` - Basic TCP server for testing  
- `src/sql_handler.rs` - SQL query parsing and validation
- `src/tables.rs` - Virtual table definitions and schemas

## Development Commands

```bash
# Build the project
cargo build

# Run with debug logging
cargo run -- --graphql-url "http://localhost:4000/graphql" --debug

# Run tests
cargo test

# Check for compilation errors
cargo check
```

## Environment Variables

- `GRAPHQL_HTTP_URL` - GraphQL server endpoint
- `RUST_LOG` - Logging configuration (debug, info, warn, error)

## Virtual Tables

The server exposes these virtual tables that map to GraphQL queries:

1. **TagValues** - Current tag values (`tagValues` GraphQL query)
2. **LoggedTagValues** - Historical tag data (`loggedTagValues` GraphQL query)  
3. **ActiveAlarms** - Current alarms (`activeAlarms` GraphQL query)
4. **LoggedAlarms** - Historical alarms (`loggedAlarms` GraphQL query)

## Testing

Currently uses a simple TCP protocol for testing. Connect with:

```bash
nc localhost 5432
# Send: username:password
# Send: SELECT * FROM tagvalues WHERE tag_name = 'TestTag';
```

## Next Steps

1. Complete virtual table implementations in `simple_server.rs`
2. Add full SQL-to-GraphQL query translation
3. Implement proper PostgreSQL wire protocol using `pgwire` crate
4. Add comprehensive error handling and logging
5. Create unit and integration tests