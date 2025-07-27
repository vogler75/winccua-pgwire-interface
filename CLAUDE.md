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
✅ **Virtual Tables**: Complete SQL-to-GraphQL translation with virtual column support
✅ **Virtual Columns**: Advanced filtering with LoggedAlarms and TagList virtual columns
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
5. **TagList** - List of available tags (`browse` GraphQL query)

## Virtual Columns

Some tables support virtual columns that are not stored in the database but map directly to GraphQL query parameters:

### LoggedAlarms Table

The `loggedalarms` table supports these virtual columns for enhanced filtering:

- **`filterString`** - Maps to GraphQL `filterString` parameter
  - Only supports `=` operator
  - Example: `WHERE filterString = 'critical'`

- **`system_name`** - Maps to GraphQL `systemNames` parameter
  - Supports `=` and `IN` operators
  - Example: `WHERE system_name IN ('Production', 'Test')`

- **`filter_language`** - Maps to GraphQL `filterLanguage` parameter  
  - Only supports `=` operator
  - Example: `WHERE filter_language = 'de-DE'`

- **`modification_time`** - Maps to GraphQL `startTime`/`endTime` parameters
  - Supports all comparison operators: `>`, `<`, `>=`, `<=`, `BETWEEN`
  - Takes priority over `timestamp` column
  - Example: `WHERE modification_time > '2024-01-01T00:00:00Z'`

**LIMIT Support**: The `LIMIT` clause maps to the GraphQL `maxNumberOfResults` parameter.

**Example Query**:
```sql
SELECT name, modification_time FROM loggedalarms 
WHERE filterString = 'alarm' 
  AND system_name IN ('System1', 'System2')
  AND filter_language = 'en-US'
  AND modification_time > CURRENT_TIME - INTERVAL '1 hour'
LIMIT 100;
```

### TagList Table

The `taglist` table supports:

- **`language`** - Virtual column for language filtering (post-GraphQL processing)
- **`display_name`** - Post-GraphQL filtering on display name (not natively supported by GraphQL)

## Debugging and Logging

When running with `--debug`, the server provides comprehensive logging including:

### GraphQL Query Logging
```
🚀 Generated GraphQL query:
📄 Query: query LoggedAlarms($systemNames: [String!], $filterString: String!, ...)
🔧 Variables: LoggedAlarmsVariables { system_names: ["Production"], ... }
```

### SQL Processing Logs
- SQL query parsing and validation
- Virtual column parameter extraction
- Filter application and result processing
- Time range handling with automatic defaults

### Authentication and Connection Logs
- Session management and token handling
- GraphQL client requests and responses
- Error handling and retry logic

## Testing

Currently uses a simple TCP protocol for testing. Connect with:

```bash
nc localhost 5432
# Send: username:password
# Send: SELECT * FROM tagvalues WHERE tag_name = 'TestTag';
```

## Next Steps

1. Implement proper PostgreSQL wire protocol using `pgwire` crate
2. Add support for more SQL features (JOINs, subqueries, etc.)
3. Enhance virtual column support for remaining tables
4. Add comprehensive integration tests with real GraphQL server
5. Performance optimization and connection pooling
6. Add more advanced filtering and aggregation capabilities